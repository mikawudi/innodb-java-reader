/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

/**
 * @author xu.zx
 */
public class GetPageFillingRateMain {

  public static void main(String[] args) {
    test();
    String createTableSql = "CREATE TABLE `tb11`\n"
        + "(`id` int(11) NOT NULL ,\n"
        + "`a` bigint(20) NOT NULL,\n"
        + "`b` varchar(64) NOT NULL,\n"
        + "PRIMARY KEY (`id`),\n"
        + "KEY `key_a` (`a`))\n"
        + "ENGINE=InnoDB;";
    String ibdFilePath = "/usr/local/mysql/data/test/t.ibd";
    try (TableReader reader = new TableReaderImpl(ibdFilePath, createTableSql)) {
      reader.open();
      System.out.println(reader.getIndexPageFillingRate(3));
      System.out.println(reader.getAllIndexPageFillingRate());
    }
  }

  public static void test() {
    String createTableSql = "CREATE TABLE `MUser` (\n" +
            "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "  `name` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,\n" +
            "  `pwd` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,\n" +
            "  `mail` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,\n" +
            "  `create_time` datetime NOT NULL,\n" +
            "  `last_login` datetime NOT NULL,\n" +
            "  `amount` int(11) NOT NULL DEFAULT '0',\n" +
            "  `status` int(4) NOT NULL DEFAULT '0',\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB";
    String ibdFilePath = "/Users/jiexu/muser.ibd";
    try (TableReader reader = new TableReaderImpl(ibdFilePath, createTableSql)) {
      reader.open();
      //System.out.println(reader.getIndexPageFillingRate(3));
      System.out.println(reader.getAllIndexPageFillingRate());
    }
  }

}
