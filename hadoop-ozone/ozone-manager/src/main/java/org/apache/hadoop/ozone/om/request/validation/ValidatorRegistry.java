package org.apache.hadoop.ozone.om.request.validation;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.util.List;
import java.util.Set;

public class ValidatorRegistry {

  private List<RequestFeatureValidator<?>> preFinalizationRequestValidators;
  private List<RequestFeatureValidator<?>> oldClientRequestValidators;
  private List<RequestFeatureValidator<?>> newClientRequestValidators;
  private List<RequestFeatureValidator<?>> genericRequestValidators;

  private List<ResponseFeatureValidator<?, ?>> preFinalizationResponseValidators;
  private List<ResponseFeatureValidator<?, ?>> oldClientResponseValidators;
  private List<ResponseFeatureValidator<?, ?>> newClientResponseValidators;
  private List<ResponseFeatureValidator<?, ?>> genericResponseValidators;

  public ValidatorRegistry(String validatorPackage) {
    initMaps(validatorPackage);
  }

  private void initMaps(String validatorPackage) {
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .setUrls(ClasspathHelper.forPackage(validatorPackage))
        .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner())
        .setExpandSuperTypes(false)
        .useParallelExecutor()
    );
    Set<Class<?>> describedValidators =
        reflections.getTypesAnnotatedWith(ValidationDescriptor.class);

    initRequestValidators();
    initResponseValidators();
  }

  private void initRequestValidators() {

  }

  private void initResponseValidators() {

  }

}
