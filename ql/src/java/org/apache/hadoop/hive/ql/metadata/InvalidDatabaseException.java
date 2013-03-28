/*
 * Copyright (c) 2010-2012 meituan.com
 * All rights reserved.
 * 
 */
package org.apache.hadoop.hive.ql.metadata;


/**
 * Generic exception class for Hive.
 *
 */

public class InvalidDatabaseException extends HiveException {
  String databaseName;

  public InvalidDatabaseException(String databaseName) {
    super();
    this.databaseName = databaseName;
  }

  public InvalidDatabaseException(String message, String databaseName) {
    super(message);
    this.databaseName = databaseName;
  }

  public InvalidDatabaseException(Throwable cause, String databaseName) {
    super(cause);
    this.databaseName = databaseName;
  }

  public InvalidDatabaseException(String message, Throwable cause, String databaseName) {
    super(message, cause);
    this.databaseName = databaseName;
  }

  public String getDatabaseName() {
    return databaseName;
  }
}
