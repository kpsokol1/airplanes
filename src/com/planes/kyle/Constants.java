package com.planes.kyle;

public class Constants {
  private static String RAW_DATA_TABLE;
  private static String CLEAN_DATA_TABLE;
  private static String CREDENTIALS_FILE_NAME;
  private static String IMAGE_FILE_PATH;

  public static void initializeConstants(final String PROPERTIES_FILE_NAME){
    RAW_DATA_TABLE = Utilities.getProperty("RAW_DATA_TABLE", PROPERTIES_FILE_NAME);
    CLEAN_DATA_TABLE = Utilities.getProperty("CLEAN_DATA_TABLE", PROPERTIES_FILE_NAME);
    CREDENTIALS_FILE_NAME = Utilities.getProperty("CREDENTIALS_FILE_NAME", PROPERTIES_FILE_NAME);
    IMAGE_FILE_PATH = Utilities.getProperty("IMAGE_FILE_PATH", PROPERTIES_FILE_NAME);
  }

  public static String getRawDataTable(){
    return RAW_DATA_TABLE;
  }

  public static String getCleanDataTable(){
    return CLEAN_DATA_TABLE;
  }
  public static String getCredentialsFileName(){
    return CREDENTIALS_FILE_NAME;
  }
  public static String getImageFilePath(){
    return IMAGE_FILE_PATH;
  }
}
