{% macro state_normalize(state) %}
    case
        when lower({{ state }}) = 'maharashtra' then 'MH'
        when lower({{ state }}) = 'rajasthan' then 'RJ'
        when lower({{ state }}) = 'uttar pradesh' then 'UP'
        when lower({{ state }}) = 'tamil nadu' then 'TN'
        when lower({{ state }}) = 'delhi' then 'DL'
        when lower({{ state }}) = 'gujarat' then 'GJ'
        when lower({{ state }}) = 'uttarakhand' then 'UK'
        when lower({{ state }}) in ('odisha', 'orissa') then 'OD'
        when lower({{ state }}) = 'chandigarh' then 'CH'
        when lower({{ state }}) = 'andhra pradesh' then 'AP'
        when lower({{ state }}) in ('jammu & kashmir', 'jammu and kashmir') then 'JK'
        when lower({{ state }}) = 'west bengal' then 'WB'
        when lower({{ state }}) = 'punjab' then 'PB'
        when lower({{ state }}) = 'assam' then 'AS'
        when lower({{ state }}) = 'bihar' then 'BR'
        when lower({{ state }}) = 'madhya pradesh' then 'MP'
        when lower({{ state }}) = 'himachal pradesh' then 'HP'
        when lower({{ state }}) = 'telangana' then 'TS'
        when lower({{ state }}) = 'chhattisgarh' then 'CT'
        when lower({{ state }}) = 'karnataka' then 'KA'
        when lower({{ state }}) = 'jharkhand' then 'JH'
        when lower({{ state }}) = 'kerala' then 'KL'
        else null
    end
{% endmacro%}