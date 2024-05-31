/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.application.ingestion.definition;

import static com.snowflake.connectors.util.variant.VariantMapper.mapVariant;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.snowflake.snowpark_java.Row;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.function.Supplier;

/** Reflection-based mapper for deserialization of resource ingestion definitions. */
class ResourceIngestionDefinitionMapper<
    R extends ResourceIngestionDefinition<I, M, C, D>, I, M, C, D> {

  private final Class<I> resourceIdClass;
  private final Class<M> resourceMetadataClass;
  private final JavaType ingestionConfigurationListType;

  private final Supplier<R> resourceFactoryMethod;

  @SuppressWarnings("unchecked")
  ResourceIngestionDefinitionMapper(Class<R> resourceClass) {
    this.resourceFactoryMethod = getNoArgsConstructorForClass(resourceClass);

    Type[] actualTypeArguments =
        ((ParameterizedType) resourceClass.getGenericSuperclass()).getActualTypeArguments();
    this.resourceIdClass = (Class<I>) actualTypeArguments[0];
    this.resourceMetadataClass = (Class<M>) actualTypeArguments[1];
    Class<C> customConfigurationClass = (Class<C>) actualTypeArguments[2];
    Class<D> destinationClass = (Class<D>) actualTypeArguments[3];

    TypeFactory typeFactory = TypeFactory.defaultInstance();
    JavaType ingestionConfigurationType =
        typeFactory.constructParametricType(
            IngestionConfiguration.class, customConfigurationClass, destinationClass);
    this.ingestionConfigurationListType =
        typeFactory.constructParametricType(List.class, ingestionConfigurationType);
  }

  R map(Row row) {
    R resource = resourceFactoryMethod.get();
    resource.setId(row.getString(0));
    resource.setName(row.getString(1));
    resource.setEnabled(row.getBoolean(2));
    resource.setParentId(row.getString(3));
    resource.setResourceId(mapVariant(row.getVariant(4), resourceIdClass));
    resource.setResourceMetadata(mapVariant(row.getVariant(5), resourceMetadataClass));
    resource.setIngestionConfigurations(
        mapVariant(row.getVariant(6), ingestionConfigurationListType));

    return resource;
  }

  private Supplier<R> getNoArgsConstructorForClass(Class<R> clazz) {
    try {
      Constructor<R> noArgConstructor = clazz.getDeclaredConstructor();
      Supplier<R> classFactoryMethod =
          () -> {
            try {
              return noArgConstructor.newInstance();
            } catch (InstantiationException e) {
              // given class is abstract or it's an interface
              throw new ResourceIngestionDefinitionInstantiationException(
                  String.format("Cannot instantiate an object of class %s.", clazz), e);
            } catch (IllegalAccessException e) {
              throw new ResourceIngestionDefinitionInstantiationException(
                  String.format(
                      "Cannot access no argument constructor of class %s. Make sure it is public.",
                      clazz),
                  e);
            } catch (InvocationTargetException e) {
              throw new ResourceIngestionDefinitionInstantiationException(
                  String.format(
                      "Cannot instantiate an object of class %s. Constructor throws exception.",
                      clazz),
                  e);
            }
          };
      classFactoryMethod
          .get(); // creating object in order to check if there is access to no args constructor
      return classFactoryMethod;
    } catch (NoSuchMethodException e) {
      throw new ResourceIngestionDefinitionInstantiationException(
          String.format("Class %s does not have no argument constructor defined.", clazz), e);
    }
  }
}
