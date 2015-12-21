package azkaban.jobtype.javautils;

import java.util.Collection;

import org.apache.commons.lang.StringUtils;

public class ValidationUtils {

  public static void validateNotEmpty(String s, String name) {
    if(StringUtils.isEmpty(s)) {
      throw new IllegalArgumentException(name + " cannot be empty.");
    }
  }

  public static void validateNotNull(Object obj, String name) {
    if(obj == null) {
      throw new IllegalArgumentException(name + " cannot be null.");
    }
  }

  public static boolean isEmpty(Collection<?> collection) {
    return collection == null || collection.isEmpty();
  }

  public static void validateNotEmpty(Collection<String> collection, String collectionName) {
    if(isEmpty(collection)) {
      throw new IllegalArgumentException(collectionName + " cannot be empty.");
    }

  }
}