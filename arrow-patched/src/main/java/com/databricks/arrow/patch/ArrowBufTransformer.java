//package com.databricks.arrow.patch;
//
//import javassist.ClassPool;
//import javassist.CtClass;
//import javassist.Modifier;
//import javassist.NotFoundException;
//
//import java.io.File;
//
///**
// * Patches Apache Arrow's ArrowBuf class to remove the 'final' modifier,
// * allowing it to be extended.
// */
//public class ArrowBufTransformer {
//
//  private static final String ARROW_BUF_CLASS = "org.apache.arrow.memory.ArrowBuf";
//
//  public static void main(String[] args) {
//    if (args.length < 2) {
//      System.err.println("Usage: ArrowBufPatcher <input-dir> <output-dir>");
//      System.exit(1);
//    }
//
//    String inputDir = args[0];
//    String outputDir = args[1];
//
//    try {
//      transform(new File(inputDir), new File(outputDir));
//    } catch (Exception e) {
//      System.err.println("[arrow-patch] ERROR: " + e.getMessage());
//      e.printStackTrace();
//      System.exit(1);
//    }
//  }
//
//  public static void transform(File inputDir, File outputDir) throws Exception {
//    if (!inputDir.exists()) {
//      throw new IllegalArgumentException("Input directory not found: " + inputDir);
//    }
//
//    outputDir.mkdirs();
//
//    ClassPool pool = ClassPool.getDefault();
//    pool.appendClassPath(inputDir.getAbsolutePath());
//
//    try {
//      CtClass arrowBuf = pool.get(ARROW_BUF_CLASS);
//      int modifiers = arrowBuf.getModifiers();
//
//      if (Modifier.isFinal(modifiers)) {
//        arrowBuf.setModifiers(Modifier.clear(modifiers, Modifier.FINAL));
//        arrowBuf.writeFile(outputDir.getAbsolutePath());
//        System.out.println("[arrow-patch] SUCCESS: Removed 'final' modifier from ArrowBuf");
//      } else {
//        System.out.println("[arrow-patch] ArrowBuf is already non-final");
//        arrowBuf.writeFile(outputDir.getAbsolutePath());
//      }
//
//      System.out.println("[arrow-patch] Patched class written to: " + outputDir);
//
//    } catch (NotFoundException e) {
//      throw new RuntimeException("ArrowBuf class not found in " + inputDir, e);
//    }
//  }
//}
