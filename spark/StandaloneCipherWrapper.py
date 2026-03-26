#!/usr/bin/env python3
import os
import ctypes
import json

lib = ctypes.CDLL(os.path.abspath("libstandalone_cipher.so"))

class CStringArray(ctypes.Structure):
    _fields_ = [("data", ctypes.POINTER(ctypes.c_char_p)),
                ("len", ctypes.c_size_t)]

lib.create_standalone_cipher.argtypes = [ctypes.c_char_p]
lib.create_standalone_cipher.restype = ctypes.POINTER(ctypes.c_void_p)

lib.encrypt.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
lib.encrypt.restype = ctypes.c_char_p

lib.decrypt.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
lib.decrypt.restype = ctypes.c_char_p

lib.destroy_standalone_cipher.argtypes = [ctypes.c_void_p]
lib.destroy_standalone_cipher.restype = None

class StandaloneCipherWrapper:
    def __init__(self, json_data):
        json_str = json.dumps(json_data)
        self.client_ptr = lib.create_standalone_cipher(json_str.encode('utf-8'))

        if not self.client_ptr:
            raise Exception("Failed to create StandaloneCipher")
        
        self.is_destroyed = False

    def encrypt(self, enc_type, value):
        if self.is_destroyed:
            raise Exception("StandaloneCipher has been destroyed and cannot be used.")
        
        encrypted_value = lib.encrypt(self.client_ptr, enc_type.encode('utf-8'), value.encode('utf-8'))
        if encrypted_value:
            return encrypted_value.decode('utf-8')
        else:
            raise Exception("Encryption failed")

    def decrypt(self, enc_type, value):
        if self.is_destroyed:
            raise Exception("StandaloneCipher has been destroyed and cannot be used.")
        
        decrypted_value = lib.decrypt(self.client_ptr, enc_type.encode('utf-8'), value.encode('utf-8'))
        if decrypted_value:
            return decrypted_value.decode('utf-8')
        else:
            raise Exception("Decryption failed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.destroy()

    def __del__(self):
        self.destroy()

    def destroy(self):
        if not self.is_destroyed and self.client_ptr:
            lib.destroy_standalone_cipher(self.client_ptr)
            self.is_destroyed = True

if __name__ == "__main__":
    cipher_keys = {'mdn': '676b73666b746b73'}
    with StandaloneCipherWrapper(cipher_keys) as cipher:
        encrypted_value = cipher.decrypt("mdn", "9rxnMb59zxDKJyTk84BSpw==")
        print("Encrypted:", encrypted_value)
