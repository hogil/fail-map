# cython_functions.pyx
cdef unsigned char HEXMAP[256]
cdef bint MAP_READY = False

cdef void _init_map():
    global MAP_READY
    if MAP_READY:
        return
    cdef int i
    for i in range(256):
        HEXMAP[i] = ord('0')
    HEXMAP[ord('0')] = ord('0')
    HEXMAP[ord('9')] = ord('1')
    HEXMAP[ord('A')] = ord('2'); HEXMAP[ord('a')] = ord('2')
    HEXMAP[ord('B')] = ord('3'); HEXMAP[ord('b')] = ord('3')
    HEXMAP[ord('C')] = ord('4'); HEXMAP[ord('c')] = ord('4')
    HEXMAP[ord('D')] = ord('5'); HEXMAP[ord('d')] = ord('5')
    HEXMAP[ord('E')] = ord('6'); HEXMAP[ord('e')] = ord('6')
    HEXMAP[ord('F')] = ord('7'); HEXMAP[ord('f')] = ord('7')
    MAP_READY = True

cdef inline unsigned char hex_to_char(unsigned char c):
    return HEXMAP[c]

def transform_line(line, int xsize):
    cdef bytes b
    if isinstance(line, bytes):
        b = line
    else:
        b = (<str>line).encode('ascii','ignore')
    if len(b) < xsize*2:
        return u""
    _init_map()
    cdef bytearray out = bytearray(xsize)
    cdef unsigned char[:] out_view = out
    cdef const unsigned char[:] mv = b
    cdef int idx, pos = 1
    for idx in range(xsize):
        out_view[idx] = hex_to_char(mv[pos]); pos += 2
    return out.decode('ascii')

def convert_hex_values_cython(list lines, int current_position, int xsize, int ysize):
    if current_position >= len(lines):
        return u""
    cdef list parts = []
    parts.append(transform_line((<str>lines[current_position])[1:], xsize))
    for i in range(1, ysize):
        if current_position + i < len(lines):
            parts.append(transform_line((<str>lines[current_position+i])[1:], xsize))
    return u",".join(parts)
