package org.apache.ozone.annotations;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ExecutableType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.SimpleAnnotationValueVisitor8;
import javax.tools.Diagnostic;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

@SupportedAnnotationTypes(
    "org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class RequestFeatureValidatorProcessor extends AbstractProcessor {

  @Override
  public boolean process(Set<? extends TypeElement> annotations,
      RoundEnvironment roundEnv) {
    System.out.println("I am here!!!!");
    for (TypeElement annotation : annotations) {
      Set<? extends Element> annotatedElements
          = roundEnv.getElementsAnnotatedWith(annotation);
      for(Element elem : annotatedElements) {
        for (AnnotationMirror methodAnnotation : elem.getAnnotationMirrors()){
          System.out.println("simpleName: " + methodAnnotation.getAnnotationType().asElement().getSimpleName());
          if (methodAnnotation.getAnnotationType().asElement().getSimpleName()
              .contentEquals("RequestFeatureValidator")){
            int expectedParamCount = -1;
            boolean hasContext = false;
            boolean isPreprocessor = false;
            for (Entry<? extends ExecutableElement, ? extends AnnotationValue>
                entry : methodAnnotation.getElementValues().entrySet()) {
              if (entry.getKey().getSimpleName().contentEquals("conditions")
                  && !entry.getValue().accept(new ConditionValidator(), null)) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "Condition is empty...");
              }
              if (entry.getKey().getSimpleName().contentEquals("contextAware")) {
                System.out.println("HAS CONTEXT!");
                hasContext = true;
              }
              if (entry.getKey().getSimpleName().contentEquals("processingPhase")) {
                String procPhase = entry.getValue().accept(new ProcessingPhaseVisitor(), null);
                if (procPhase.equals("PRE_PROCESS")) {
                  isPreprocessor = true;
                  expectedParamCount = 1;
                } else if (procPhase.equals("POST_PROCESS")){
                  isPreprocessor = false;
                  expectedParamCount = 2;
                }
              }
            }
            if (hasContext) {
              expectedParamCount++;
            }
            if (elem.getKind() != ElementKind.METHOD) {
              processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                  "Annotated element is not a method...");
            }
            if (!elem.getModifiers().contains(Modifier.STATIC)) {
              processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                  "Validator method has to be static...");
            }
            if (isPreprocessor && !((ExecutableElement) elem).getReturnType().toString()
                .equals("org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest")) {
              processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                  "Validator method has to return OMRequest...");
            }
            if (!isPreprocessor && !((ExecutableElement) elem).getReturnType().toString()
                .equals("org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse")) {
              processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                  "Validator method has to return OMResponse...");
            }
            List<? extends TypeMirror> paramTypes =
                ((ExecutableType) elem.asType()).getParameterTypes();
            int realParamCount =
                paramTypes.size();
            if (realParamCount != expectedParamCount) {
              processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                  "Unexpected parameter count. Expected: " + expectedParamCount
                      + "; found: " + realParamCount);
            }
            paramTypes.forEach(t -> {
              if (!t.getKind().equals(TypeKind.DECLARED)) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "Unexpected parameter type, it has to be a declared type."
                        + " Found: " + t);
              }
            });
            int contextOrder = -1;
            if (isPreprocessor) {
              contextOrder = 1;
            } else {
              contextOrder = 2;
              if (paramTypes.size()>=2 && !paramTypes.get(1).toString()
                  .equals("org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse")) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "Validator method second param has to be OMResponse...");
              }
            }
            if (paramTypes.size()>=1 && !paramTypes.get(0).toString()
                .equals("org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest")) {
              processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                  "Validator method first param has to be OMRequest...");
            }
            if (paramTypes.size()>=contextOrder+1 && !paramTypes.get(contextOrder).toString()
                .equals("org.apache.hadoop.ozone.om.request.validation.ValidationContext")) {
              processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                  "Validator method last param has to be ValidationContext...");
            }
          }
        }
      }
    }

    return true;
  }

  class ConditionValidator
      extends SimpleAnnotationValueVisitor8<Boolean, Void> {

    ConditionValidator() {
      super(Boolean.TRUE);
    }

    @Override
    public Boolean visitArray(List<? extends AnnotationValue> vals,
        Void unused) {
      if (vals.isEmpty()) {
        return Boolean.FALSE;
      }
      return Boolean.TRUE;
    }
  }

  class ProcessingPhaseVisitor
      extends SimpleAnnotationValueVisitor8<String, Void> {

    ProcessingPhaseVisitor() {
      super("UNKNOWN");
    }

    @Override
    public String visitEnumConstant(VariableElement c, Void unused) {
      if (c.getSimpleName().contentEquals("PRE_PROCESS")) {
        return "PRE_PROCESS";
      } else if (c.getSimpleName().contentEquals("POST_PROCESS")) {
        return "POST_PROCESS";
      }
      throw new IllegalStateException("Method processing phase is unknown...");
    }
  }
}
