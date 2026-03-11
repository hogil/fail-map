#!/usr/bin/env python3
"""
Cython 빌드 스크립트 — 실행만 하면 끝:
    python setup.py
"""
import os, sys, subprocess

def main():
    src_dir = os.path.dirname(os.path.abspath(__file__))
    pyx = os.path.join(src_dir, "cython_functions.pyx")
    if not os.path.exists(pyx):
        print(f"[error] {pyx} not found")
        sys.exit(1)

    # Cython 설치 확인
    try:
        import Cython
    except ImportError:
        print("[setup] installing cython...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "cython"])

    from setuptools import setup, Extension
    from Cython.Build import cythonize

    os.chdir(src_dir)
    setup(
        script_args=["build_ext", "--inplace"],
        ext_modules=cythonize("cython_functions.pyx", language_level=3),
    )
    print("\n[done] cython_functions built successfully")

if __name__ == "__main__":
    main()
