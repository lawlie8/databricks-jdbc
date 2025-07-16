package com.databricks.jdbc.telemetry.latency;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportLatencyLog;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class DatabricksMetricsTimedProcessor {

  @SuppressWarnings("unchecked")
  public static <T> T createProxy(T obj) {
    Class<?> clazz = obj.getClass();
    Class<?>[] interfaces = clazz.getInterfaces();
    return (T)
        Proxy.newProxyInstance(
            clazz.getClassLoader(), interfaces, new TimedInvocationHandler<>(obj));
  }

  private static class TimedInvocationHandler<T> implements InvocationHandler {
    private final T target;

    public TimedInvocationHandler(T target) {
      this.target = target;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        if (method.isAnnotationPresent(DatabricksMetricsTimed.class)) {
          long startTime = System.currentTimeMillis();
          Object result = method.invoke(target, args);
          long executionTime = System.currentTimeMillis() - startTime;
          exportLatencyLog(executionTime); // Log is exported ONLY here.
          return result;
        } else {
          // For methods like deleteSession(), this block is executed.
          return method.invoke(target, args);
        }
      } catch (InvocationTargetException e) {
        // catch the exception from either path, unwraps it, and
        // throws the real cause. It does not log latency.
        throw e.getCause();
      }
    }
  }
}
