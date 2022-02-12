package org.dde.zju.base;

import lombok.SneakyThrows;
import org.dde.zju.kczy.util.FieldOperator;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class BaseObject {

    public String toString(boolean callSuper) throws IllegalAccessException {
        List<Field> allFields = FieldOperator.getAllFields(this.getClass(), callSuper);
        List<String> values = new ArrayList<>();
        for (Field allField : allFields) {
            String fieldValue = getFieldValue(allField);
            values.add(fieldValue);
        }
        return String.join(Constant.STRING_SEPORATOR, values);
    }

    @SneakyThrows(IllegalAccessException.class)
    @Override
    public String toString() {
        return this.toString(true);
    }

    private String getFieldValue(Field field) throws IllegalAccessException {
        // 设置字段可访问， 否则无法访问private修饰的变量值
        field.setAccessible(true);
        // 获取指定对象的当前字段的值
        return String.valueOf(field.get(this));
    }

    @SneakyThrows
    public void convertFromStr(String str) {
        this.convertFromStr(str, true);
    }

    public void convertFromStr(String str, boolean callSuper) throws IllegalAccessException {
        List<Field> allFields = FieldOperator.getAllFields(this.getClass(), callSuper);
        String[] values = str.split(Constant.STRING_SEPORATOR);
        if (allFields.size() != values.length) throw new RuntimeException("Value size should be equal to Field Size");
        for(int i=0; i<allFields.size(); i++) {
            Field field = allFields.get(i);
            field.setAccessible(true);
            if (String.class.equals(field.getDeclaringClass())) {
                field.set(this, values[i]);
            } else if (Double.class.equals(field.getDeclaringClass())) {
                field.setDouble(this, Double.parseDouble(values[i]));
            } else if (Integer.class.equals(field.getDeclaringClass())) {
                field.setInt(this, Integer.parseInt(values[i]));
            } else if (Long.class.equals(field.getDeclaringClass())) {
                field.setLong(this, Long.parseLong(values[i]));
            } else if (Boolean.class.equals(field.getDeclaringClass())) {
                field.setBoolean(this, Boolean.parseBoolean(values[i]));
            } else {
                throw new IllegalStateException("Unsupport class type: " + field.getDeclaringClass());
            }
        }
    }
}
