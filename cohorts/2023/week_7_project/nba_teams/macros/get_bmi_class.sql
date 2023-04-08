 {#
    This macro returns the class for calculated BMI 
#}

{% macro get_bmi_class(BMI) -%}

    case
        WHEN {{ BMI }} < 16.5 THEN 'Severely underweight'
        WHEN {{ BMI }} >= 16.5 AND {{ BMI }} < 18.5 THEN 'Underweight'
        WHEN {{ BMI }} >= 18.5 AND {{ BMI }} < 25 THEN 'Normal weight'
        WHEN {{ BMI }} >= 25 AND {{ BMI }} < 30 THEN 'Overweight'
        WHEN {{ BMI }} >= 30 AND {{ BMI }} < 35 THEN 'Class I Obesity'
        WHEN {{ BMI }} >= 35 AND {{ BMI }} < 40 THEN 'Class II Obesity'
        ELSE 'Class III Obesity'
    end 

{%- endmacro %}

              