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
        // String[] values = str.split(Constant.STRING_SEPORATOR);
        String[] values = str.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1);
        if (allFields.size() != values.length) throw new RuntimeException("Value size should be equal to Field Size");
        for (int i = 0; i < allFields.size(); i++) {
            Field field = allFields.get(i);
            field.setAccessible(true);
            Class<?> fieldType = field.getType();
            if (byte.class.equals(fieldType) || Byte.class.equals(fieldType)) {
                field.setByte(this, Byte.parseByte(modifyEmptyNumeric(values[i])));
            } else if (short.class.equals(fieldType) || Short.class.equals(fieldType)) {
                field.setShort(this, Short.parseShort(modifyEmptyNumeric(values[i])));
            } else if (int.class.equals(fieldType) || Integer.class.equals(fieldType)) {
                field.setInt(this, Integer.parseInt(modifyEmptyNumeric(values[i])));
            } else if (long.class.equals(fieldType) || Long.class.equals(fieldType)) {
                field.setLong(this, Long.parseLong(modifyEmptyNumeric(values[i])));
            } else if (float.class.equals(fieldType) || Float.class.equals(fieldType)) {
                field.setFloat(this, Float.parseFloat(modifyEmptyNumeric(values[i])));
            } else if (double.class.equals(fieldType) || Double.class.equals(fieldType)) {
                field.setDouble(this, Double.parseDouble(modifyEmptyNumeric(values[i])));
            } else if (char.class.equals(fieldType) || Character.class.equals(fieldType)) {
                field.setChar(this, modifyEmptyChar(values[i]));
            } else if (boolean.class.equals(fieldType) || Boolean.class.equals(fieldType)) {
                field.setBoolean(this, Boolean.parseBoolean(values[i]));
            } else if (String.class.equals(fieldType)) {
                field.set(this, values[i]);
            } else {
                throw new IllegalStateException("Unsupport class type: " + fieldType);
            }
        }
    }

    public String modifyEmptyNumeric(String numericStr) {
        if (numericStr == null || numericStr.length() == 0) {
            return "0";
        }
        return numericStr;
    }

    public char modifyEmptyChar(String numericStr) {
        if (numericStr == null || numericStr.length() == 0) {
            return ' ';
        }
        return numericStr.charAt(0);
    }


}
