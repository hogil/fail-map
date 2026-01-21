    # [NEW] Parallel Listing & Filtering for Secondary Bucket
    def fetch_secondary_data_parallel(self, dataset_A):
        """
        1. Identify unique Lot/Wafer/Date from dataset_A keys.
        2. Parallel ListObjectsV2 on Secondary Bucket using specific prefix.
        3. Filter files by Time Difference (0~10s).
        4. Download matched files.
        """
        if not dataset_A:
            return {}

        # Group by "YYYYMMDD/LOT_WAFER" to minimize list requests
        # Target Format: YYYYMMDD/ABC123P_W01_...
        
        # Helper to convert Bucket A filename info to Bucket B Prefix
        def _get_search_prefix(d):
            # We need original filename or parsed fields
            # dataset_A items have 'root', 'step', 'wafer' but we need original filename parts
            # Let's rely on the original key if available, or reconstruct from fields?
            # 'd' comes from process_file_content return
            # We need to access the filename that generated 'd'. 
            # But 'd' doesn't have the full filename, only parsed fields.
            # However, in run_pipeline, we iterate dataset_all which has 'root', 'step', 'wafer'.
            
            # PROBLEM: process_file_content does not return original filename.
            # We need to pass it or rely on heuristic reconstruction.
            # Let's assume we can reconstruct or we added it.
            # Actually, let's just parse the 'p1' (Lot?) and 'wafer' fields if they are reliable.
            # Wait, user example: "01_ABC123-00P..."
            # 'wafer' in dataset might be "W01" or "01". 
            # In `find_initial_values_from_lines`: step, wafer = wfid.split... 
            # This depends on file content, not filename.
            
            # Let's look at how we can get the original filename parts.
            # In run_pipeline, we have: tagged_pairs.append((tok, p1, p2, name, text))
            # But process_file_content only returns parsed data.
            # FIX: We should add 'orig_filename' to dataset_A in run_pipeline
            
            orig_name = d.get('orig_name', '')
            if not orig_name: return None, None
            
            # Filename: 01_ABC123-00P_N_20260121_025936.Z
            try:
                base = os.path.basename(orig_name)
                parts = base.split('_')
                if len(parts) < 4: return None, None
                
                wafer_num = parts[0]  # "01"
                lot_part = parts[1]   # "ABC123-00P"
                date_part = parts[-2] # "20260121"
                
                # Lot conversion: ABC123-00P -> ABC123P
                if "-00" in lot_part:
                    lot_base, suffix = lot_part.split("-00", 1)
                    new_lot = lot_base + suffix
                else:
                    new_lot = lot_part
                
                new_wafer = f"W{wafer_num}"
                
                # Prefix: 20260121/ABC123P_W01_
                prefix = f"{date_part}/{new_lot}_{new_wafer}_"
                return prefix, date_part
            except:
                return None, None

        groups = {}
        for d in dataset_A:
            prefix, day = _get_search_prefix(d)
            if not prefix: continue
            
            if prefix not in groups:
                groups[prefix] = []
            groups[prefix].append(d)

        # 1. Parallel List
        found_files = []
        
        def _list_prefix(prefix):
            try:
                # Optimized list: getting only files starting with specific Lot+Wafer
                resp = self.client.list_objects_v2(Bucket=self.cfg.secondary_bucket_name, Prefix=prefix)
                return resp.get('Contents', [])
            except:
                return []

        with ThreadPoolExecutor(max_workers=32) as ex:
            futures = {ex.submit(_list_prefix, p): p for p in groups.keys()}
            for fut in as_completed(futures):
                found_files.extend(fut.result())

        # 2. In-Memory Filter
        # Map: (Token, Wafer, Step) -> Dataset Item? No, unique ID needed.
        # Let's map by original filename or constructed key.
        
        # We need to match found_file (Bucket B) to dataset item (Bucket A)
        # Bucket B: 20260121/ABC123P_W01_20260121_025938.gz
        
        # Build lookup from dataset_A
        # Key: (Lot_converted, Wafer_converted, Date) -> Item
        lookup = {}
        for d in dataset_A:
            prefix, _ = _get_search_prefix(d) # Re-calculate or cache
            if not prefix: continue
            # prefix is "20260121/ABC123P_W01_"
            # We can use this prefix as key? No, need time comparison.
            
            # Key = prefix (which contains Lot+Wafer)
            if prefix not in lookup: lookup[prefix] = []
            lookup[prefix].append(d)

        keys_to_download = []
        
        for obj in found_files:
            key = obj['Key'] # 20260121/ABC123P_W01_20260121_025938.gz
            
            # Extract Time from B
            try:
                # ..._025938.gz
                base = os.path.splitext(os.path.basename(key))[0]
                ts_str = base.split('_')[-1] # 025938
                date_str = base.split('_')[-2] # 20260121
                dt_b = datetime.strptime(f"{date_str}_{ts_str}", "%Y%m%d_%H%M%S")
                
                # Find matching A items
                # The file key must start with one of our prefixes
                # Optimization: Check which prefix this key matches
                # Since we listed BY prefix, we know it matches SOME prefix.
                
                # Heuristic: Reconstruct prefix from key to find lookup
                # Key: 20260121/ABC123P_W01_...
                # Prefix: 20260121/ABC123P_W01_
                parts = base.split('_')
                # ABC123P, W01, 20260121, 025938
                # Prefix reconstruction might be tricky if Lot has underscores.
                # But we know structure: {LOT}_{WAFER}_{DATE}_{TIME}
                
                # Let's assume standard format:
                # parts[-1] is time
                # parts[-2] is date
                # parts[-3] is wafer
                # remainder is lot
                
                lot_wafer_part = "_".join(parts[:-2]) # ABC123P_W01
                prefix_candidate = f"{date_str}/{lot_wafer_part}_"
                
                if prefix_candidate in lookup:
                    candidates = lookup[prefix_candidate]
                    for item in candidates:
                        dt_a = _parse_stime_dt(item.get('stime'))
                        if dt_a:
                            diff = (dt_b - dt_a).total_seconds()
                            if 0 <= diff <= 10:
                                # Match found!
                                meta_key = (item.get('token'), item.get('wafer'), item.get('step'))
                                keys_to_download.append((key, meta_key))
            except:
                pass

        # 3. Parallel Download
        results = {}
        def _dl(args):
            k, meta_key = args
            try:
                # Extension is .gz, need to decompress
                obj = self.client.get_object(Bucket=self.cfg.secondary_bucket_name, Key=k)
                with gzip.GzipFile(fileobj=obj['Body']) as gz:
                    data = json.load(gz)
                return (meta_key, data)
            except:
                return None

        with ThreadPoolExecutor(max_workers=64) as ex:
            for res in ex.map(_dl, keys_to_download):
                if res:
                    m_key, data = res
                    results[m_key] = data
                    
        return results
