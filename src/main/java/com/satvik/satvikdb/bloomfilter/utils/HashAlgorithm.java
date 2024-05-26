package com.satvik.satvikdb.bloomfilter.utils;

public enum HashAlgorithm {
  SHA3_512("SHA3-512"),
  SHA_1("SHA-1"),
  SHA_384("SHA-384"),
  SHA3_384("SHA3-384"),
  SHA_224("SHA-224"),
  SHA_512_256("SHA-512/256"),
  SHA_256("SHA-256"),
  MD2("MD2"),
  SHA_512_224("SHA-512/224"),
  SHA3_256("SHA3-256"),
  SHA_512("SHA-512"),
  SHA3_224("SHA3-224"),
  MD5("MD5");

  final String name;

  HashAlgorithm(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
}
