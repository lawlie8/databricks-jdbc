package com.jayant;

import com.jayant.testparams.TestParams;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public class ReflectionUtils {
  private static final Set<Object[]> NO_PARAMS = new HashSet<>();

  static {
    NO_PARAMS.add(new Object[] {});
  }

  public static Stream<Arguments> provideMethodsForClass(Class<?> clazz, TestParams testParams) {
    Set<Map.Entry<String, Integer>> acceptedKnownDiffs = testParams.getAcceptedKnownDiffs();
    Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap =
        testParams.getFunctionToArgsMap();
    Set<Arguments> argumentsStream = new HashSet<>();
    Set<Method> methodsWhereArgsAreNotProvided = new HashSet<>();
    Set<Map.Entry<String, Integer>> argumentsAdded = new HashSet<>();
    try {
      Method[] methods = clazz.getMethods();
      for (Method method : methods) {
        String methodName = method.getName();
        int parameterCount = method.getParameterCount();
        Map.Entry<String, Integer> methodWithArgs = Map.entry(methodName, parameterCount);
        if (argumentsAdded.contains(methodWithArgs)) {
          continue;
        }
        if (acceptedKnownDiffs.contains(methodWithArgs)) {
          continue;
        }
        Set<Object[]> paramSet = functionToArgsMap.getOrDefault(methodWithArgs, NO_PARAMS);
        for (Object[] params : paramSet) {
          if (parameterCount != params.length) {
            // This will ensure that we do not skip any methods in the class
            methodsWhereArgsAreNotProvided.add(method);
          }
          Arguments arguments = Arguments.of(methodName, params);
          argumentsStream.add(arguments);
        }
        argumentsAdded.add(methodWithArgs);
      }
    } catch (SecurityException e) {
      e.printStackTrace();
    }

    if (!methodsWhereArgsAreNotProvided.isEmpty()) {
      throw new RuntimeException(
          "Method parameters were not provided for:\n"
              + methodsWhereArgsAreNotProvided.stream()
                  .map(Method::toString)
                  .collect(Collectors.joining("\n")));
    }

    return argumentsStream.stream();
  }

  public static Object executeMethod(Object object, String methodName, Object[] args)
      throws SQLException {
    Object result;
    try {
      // Get the method by its name and parameter types
      Method method = null;
      Method[] methods = object.getClass().getMethods();
      for (Method m : methods) {
        if (m.getName().equals(methodName)) {
          // Check if the parameter types match
          Class<?>[] paramTypes = m.getParameterTypes();
          if (paramTypes.length == args.length) {
            int matched = 0;
            for (int i = 0; i < paramTypes.length; i++) {
              // Handle both primitive and wrapper types (autoboxing)
              if (paramTypes[i].isPrimitive()) {
                if (isPrimitiveCompatible(paramTypes[i], args[i].getClass())) {
                  matched++;
                }
              } else {
                // if args[i] is null assume it is of the correct type
                if (args[i] == null || paramTypes[i].isAssignableFrom(args[i].getClass())) {
                  matched++;
                }
              }
            }
            if (matched == paramTypes.length) {
              method = m;
              break;
            }
          }
        }
      }

      // If no matching method was found, throw an exception
      if (method == null) {
        throw new NoSuchMethodException("No matching method found: " + methodName);
      }

      // Invoke the method dynamically with the arguments
      try {
        result = method.invoke(object, args);
      } catch (InvocationTargetException e) {
        result = e.getCause();
      }
    } catch (Exception e) {
      throw new SQLException("Error executing method: " + methodName, e);
    }
    return result;
  }

  // Helper method to check compatibility between primitive and wrapper types
  private static boolean isPrimitiveCompatible(Class<?> primitiveType, Class<?> wrapperType) {
    if (primitiveType == int.class && wrapperType == Integer.class) return true;
    if (primitiveType == boolean.class && wrapperType == Boolean.class) return true;
    if (primitiveType == long.class && wrapperType == Long.class) return true;
    if (primitiveType == short.class && wrapperType == Short.class) return true;
    if (primitiveType == byte.class && wrapperType == Byte.class) return true;
    if (primitiveType == char.class && wrapperType == Character.class) return true;
    if (primitiveType == float.class && wrapperType == Float.class) return true;
    if (primitiveType == double.class && wrapperType == Double.class) return true;
    return false;
  }
}
