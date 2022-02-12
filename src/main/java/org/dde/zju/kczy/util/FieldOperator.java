package org.dde.zju.kczy.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FieldOperator {

    public static List<Field> getAllFields(Class clazz, boolean callSuper) {
        List<Field> allFields = new ArrayList<>(Arrays.asList(clazz.getDeclaredFields()));
        // 获取所有父类的字段， 父类中的字段需要逐级获取
        Class clazzSuper = clazz.getSuperclass();

        // 如果父类不是object，表明其继承的有其他类。 逐级获取所有父类的字段
        if (callSuper) {
            while (clazzSuper != Object.class) {
                allFields.addAll(Arrays.asList(clazzSuper.getDeclaredFields()));
                clazzSuper = clazzSuper.getSuperclass();
            }
        }
        return allFields;
    }



}
