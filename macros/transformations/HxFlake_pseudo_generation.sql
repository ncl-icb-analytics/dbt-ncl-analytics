{% macro hxflake_pseudo_generation(sk_patient_id) %}
LEFT(
    SUBSTR(
        TRIM(
            REVERSE(
                RIGHT(
                    LPAD(
                        TO_CHAR({{ sk_patient_id }}, 'XXXXXXXXX'),
                        9,
                        0
                    ),
                    10
                )
            )
        ) || '0000000000',
        1,
        2
    ) || '-' || RPAD(
        SUBSTR(
            TRIM(
                REVERSE(
                    RIGHT(
                        LPAD(
                            TO_CHAR({{ sk_patient_id }}, 'XXXXXXXXX'),
                            9,
                            0
                        ),
                        10
                    )
                )
            ) || '0000000000',
            3,
            4
        ),
        3,
        '0'
    ) || '-' || SUBSTR(
        TRIM(
            REVERSE(
                RIGHT(
                    LPAD(
                        TO_CHAR({{ sk_patient_id }}, 'XXXXXXXXX'),
                        9,
                        0
                    ),
                    10
                )
            )
        ) || '0000000000',
        6,
        3
    ) || '0000000000',
    10
)
{% endmacro %}