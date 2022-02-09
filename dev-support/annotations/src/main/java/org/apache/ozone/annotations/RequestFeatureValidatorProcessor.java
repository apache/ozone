package org.apache.ozone.annotations;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ExecutableType;
import javax.tools.Diagnostic;
import java.util.Set;

@SupportedAnnotationTypes(
    "org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class RequestFeatureValidatorProcessor extends AbstractProcessor {

  @Override
  public boolean process(Set<? extends TypeElement> annotations,
      RoundEnvironment roundEnv) {

    for (TypeElement annotation : annotations) {
      Set<? extends Element> annotatedElements
          = roundEnv.getElementsAnnotatedWith(annotation);
      for(Element element : annotatedElements) {
        ExecutableType elem = ((ExecutableType) element.asType());
        if (elem.getParameterTypes().size()!=3) {
          processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
              "testing error...", element);
        }
      }
    }

    return true;
  }
}
