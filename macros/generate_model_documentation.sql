{% macro generate_model_documentation(model_folder=None, my_unique_model=None) %}

    {######################################################
    Author: Joshua Garza  20231129                       
    Purpose: Fresh Start                            
    Generates model yaml for sources of any model.      
    Some component macros borow heavily from 
    the codegen dbt package.
    #######################################################
    HISTORY:                                            
    Need to modify this package with parameters for     
    model paths as well as improve the loop for sources.
    ######################################################}

    {##JG##% set model_paths = get_model_paths() %##JG##}
    {% set my_models = codegen.get_models() %}
    {% set my_model = my_unique_model %}    


    {% if model_folder is none and my_unique_model is none %}
        {{ log("\nPlease provide either a Model Folder or a Model to generate documentation", info=True) }}
        {% do return("Exception") %}            
    {% elif model_folder and my_unique_model %}    
        {{ log("\nYou've given me both a model folder and a model. \nPlease, provide only one.\n" 
            ~  " model_folder = " ~ model_folder ~ " model = " ~ my_unique_model ~"\n", info=True) }}
        {% do return(model_folder) %}
    {% elif model_folder is none and my_unique_model %}
        {{ log("\nPlease wait a moment while I attempt to generate your documentation.\n 
        The model you are requesting " ~ my_unique_model, info=True) }}
        {% set my_single_value_array = [my_unique_model] %}
        {% set column_comment = get_source_comments_for_model(my_single_value_array, model_folder) %}
    {% elif model_folder and (my_unique_model is none) %}
        {{ log("\nPlease wait a moment while I attempt to generate your documentation.\n 
        The model folder you are requesting is " ~ model_folder, info=True) }}
        {% if model_folder %}
            {% set models_to_generate = codegen.get_models(directory=model_folder ~ "/mart") %}
            {% set current_model_name = models_to_generate[0] %}
            {% for model in models_to_generate %}
                {% set current_model_name = model[0] %}
                {% set my_model_names = [model] %}
                {% set column_comment = get_source_comments_for_model(my_model_names, model_folder) %}
            {% endfor %}
        {% endif %}
    {% else %}
        {{ log(" Something does not compute :-(  = " , info=True) }}
        {% do return("FOOBAR") %} 
    {% endif %}
{% endmacro %}

{% macro get_source_comments_for_model(my_model_names, model_folder) %}
    {% set globals = namespace(processed_sources=[]) %}
    {% set my_array = [] %}
    {# Run for a unique model#}
    {% if model_folder is none and my_model_names %}
        {% set models_to_iterate = [] -%}
        {% for node in graph.nodes.values() -%}
            {% set model_name_to_gen = node.name %}
            {% if model_name_to_gen in my_model_names and model_name_to_gen not in models_to_iterate %}
                {% set source_name_array = [] %}
                {% for mysourcenode in graph.sources.values() -%}
                    {% set current_source_name = mysourcenode.source_name %}
                    {% set model = mysourcenode.fqn[3] %}
                    {% set resource_type = mysourcenode.resource_type %}
                    {% set sources = [] -%}            
                    {%- if model_name_to_gen in my_model_names and resource_type == 'source' and current_source_name not in source_name_array -%}  
                            {%- do sources.append(source(mysourcenode.source_name, mysourcenode.name)) -%} 
                            select * from (
                            {%- for source in sources %}
                                    {%  if source not in my_array %}
                                        select * from {{ source }}
                                        {{ split_string_contents( source | string , my_model_names) }}
                                    {% endif %}
                            {% endfor %}
                            {% do my_array.append( source(mysourcenode.source_name, mysourcenode.name) ) -%}    
                                )
                    {%- endif -%}
                    {%- do source_name_array.append(current_source_name) -%}
                {% endfor %}

            {%- endif -%}
            {%- do models_to_iterate.append(model_name_to_gen) -%}
        {% endfor %}    
        {% do return(model_folder) %}
    {% else %}
    {# Run for a model folder#}
        {% set source_name_array = [] %}
        {% for mysourcenode in graph.sources.values() -%}
            {% set internal_counter = loop.index0 %}
            
            {% set model_source_folder = mysourcenode.fqn[1] %}
            {% set model = mysourcenode.fqn[3] %}
            {% set current_source_name = mysourcenode.source_name %}
            {% set resource_type = mysourcenode.resource_type %}
            {% set sources = [] -%}            
            {##JG##% do log("----->internal_counter = " ~ internal_counter | string 
                ~ " model_source_folder = " ~ model_source_folder, info=true) %##JG##}
            {% if current_source_name not in globals.processed_sources %}

                {%- if model_source_folder == model_folder and resource_type == 'source' 
                    and current_source_name not in source_name_array -%}
                    {%- do sources.append(source(mysourcenode.source_name, mysourcenode.name)) -%} 
                    select * from (
                    {%- for source in sources %}
                            {%  if source not in my_array %}
                                select * from {{ source }}
                                {{ split_string_contents( source | string , my_model_names, counter = internal_counter, globals=globals ) }}
                                {% do globals.processed_sources.append(current_source_name) %}
                            {% endif %}
                    {% endfor %}
                    {% do my_array.append( source(mysourcenode.source_name, mysourcenode.name) ) -%}    
                        )
                {%- endif -%}
                {%- do source_name_array.append(current_source_name) -%}
                {##JG##% do log("GLOBALS = " ~ globals | string, info=true) %##JG##}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro split_string_contents(fully_qualified_string, my_model_names, counter, globals, my_model_array=[]) %}
    {##JG##% do log("GLOBALS FROM SPLIT STRING = " ~ globals | string, info=true) %##JG##}
    {##JG##% do log("----->initial split_string_contents call. counter = " ~ counter | string, info=true) %##JG##}
    {% set parts = fully_qualified_string.split('.') %}
    {% set database = parts[0] %}
    {% set schema = parts[1] %}
    {% set table = parts[2] %}
    {% do my_model_array.append( my_model_names ) -%}
    {{ database }}, {{ schema }}, {{ table }}
    {{ fetch_db_metadata_for_sources(database_name = database , schema_name = schema,
        source_name = table, my_model_names = my_model_names) }}
    {##JG##% do log("<------Yaml created. counter = " ~ counter | string, info=true) %##JG##}
{% endmacro %}

{% macro fetch_db_metadata_for_sources(database_name, schema_name, source_name, my_model_names, columns_dict={}, my_model_array=[] ) %}
    {##JG##% do log("----->run db query for source_name = " ~ source_name | string, info=true) %##JG##}
    {% do my_model_array.append( my_model_names | string ) -%}   
    {%  if model_name not in my_model_array %}
    {% if execute %}
            {% set sql %}
                select
                    table_name,
                    column_name,
                    coalesce(comment, '') as description,
                    data_type
                from {{ database_name }}.information_schema.columns
                where table_schema = '{{ schema_name | upper }}'
                and lower(table_name) = lower('{{ source_name }}')
                order by table_name, column_name;
            {% endset %}

            {%- call statement('generator', fetch_result=True) -%}
            {{ sql }}
            {%- endcall -%}
      
        {% set column_details = load_result('generator')['data'] %}
        {% set model_columns = columns_dict.get(model_name, []) %}

        {% for row in column_details %}
            {% set new_column_detail = {
                'my_model_name': my_model_names,
                'table_name': row[0],
                'column_name': row[1],
                'description': row[2],
                'data_type': row[3]
            } %}
            
            {# Check if this column detail is already in the list for this model #}
            {% if new_column_detail not in model_columns %}
                {% do model_columns.append(new_column_detail) %}
            {% endif %}
        {% endfor %}
        
        {% if model_columns %}
            {##JG##% do log("----->Looping through model_columns = " ~ model_columns, info=true) %##JG##}
            {{ generate_model_yaml_header(model_names = my_model_names, upstream_descriptions=True, my_table_name = model_columns  ) }}
        {% endif %}
    {% endif %}
    {% do my_model_array.append( model_name ) -%}   
    {% endif %}  
    {% do return(my_model_names) %}  
{% endmacro %}

{% macro generate_model_yaml_header(my_table_name, model_names, upstream_descriptions=False ) %}
    {#JG## This is just a copy of the dbt codegen package where I make several modifications ##JG##}
    {# Create a dictionary from your imported columns for easy lookup #}
    {% set imported_columns_dict = {} %}
    {% for column in my_table_name.columns %}
        {% do imported_columns_dict.update({column.name: column}) %}
    {% endfor %}

    {% for column in my_table_name %}
        {% do imported_columns_dict.update({column.column_name: column}) %}
    {% endfor %}

    {% set model_yaml=[] -%}
    {% set columns_to_iterate=[] -%}
    
    {% do model_yaml.append('') -%}
    {%- do model_yaml.append('version: 2') -%}
    {%- do model_yaml.append('# groups')  -%}
    {% do model_yaml.append('groups: ' ) -%}
    {% do model_yaml.append('  - name: ' ~ "your_group_name") -%}
    {% do model_yaml.append('    owner: ') -%}
    {% do model_yaml.append('      name: ' ~ "example_owner_name") -%}
    {% do model_yaml.append('      email: '  ~ "example_owner_email") -%}
    {% do model_yaml.append('') -%}
    {% do model_yaml.append('models :') -%}

    {% if model_names is string %}
        {{ exceptions.raise_compiler_error("The `model_names` argument must always be a list, even if there is only one model.") }}
    {% else %}
        {% for model in model_names %}
                {% do model_yaml.append('  - name: ' ~ model | lower) %}
                {% do model_yaml.append('    group: '  ~ "your_group_name") %}
                {% do model_yaml.append('    access: '   ~ "your_access_type") %}
                {% do model_yaml.append('    description: ' ~ "your_description") %}
                {% do model_yaml.append('    meta: ') %}
                {% do model_yaml.append('      contains_pii: ' ~ "true/false # Specify true or false") %}
                {% do model_yaml.append('      owner: ' ~ "model_owner") %}
                {% do model_yaml.append('    columns: ') %}
                {% set relation=ref(model) %}
                {%- set columns = adapter.get_columns_in_relation(relation) -%}
                {%- set my_relation_identifier = relation.identifier -%}
                {% set column_desc_dict =  codegen.build_dict_column_descriptions(model) if upstream_descriptions else {} %}
                {% set dictionary_models_to_iterate = [] -%}
                {% if my_relation_identifier in model %} 
                    {% for column in columns %}
                        {% if column.name in imported_columns_dict and column.name not in columns_to_iterate %}
                            {% set imported_column = imported_columns_dict[column.name] %}
                            {% set model_yaml = generate_column_yaml_descriptions(imported_column, model_yaml, column_desc_dict) %}
                        {% else %}
                            {% set model_yaml = generate_column_yaml_descriptions(column, model_yaml, column_desc_dict) %}
                        {% endif %}
                        {% do columns_to_iterate.append( column.name ) -%}
                    {% endfor %}
                {% endif %}    
        {% endfor %}
    {% endif %}

    {% if execute %}

        {% set joined = model_yaml | join ('\n') %}
        {{ log(joined, info=True) }}
        {##JG##% do return(joined) %##JG##}
    {% endif %}
{% endmacro %}

{% macro generate_column_yaml_descriptions(column, model_yaml, column_desc_dict, parent_column_name="") %}
    {% for column_name, column in column_desc_dict.items() %}
        {% set column_description = column.description %}
    {% endfor %}

    {% if parent_column_name %}
        {% set column_name = parent_column_name ~ "." ~ column.name %}
    {% else %}
        {% set column_name = column.name %}
    {% endif %}



    {% if parent_column_name %}
        {% set column_name = parent_column_name ~ "." ~ column.name %}
    {% elif column.name %}
        {% set column_name = column.name %}
    {% else %}
        {% set column_name = column.column_name | default('default_column_name') %}
    {% endif %}


    {% set column_description = column.description | default('') %}
    {% set column_datatype = column.data_type | default('') %}
    {% do model_yaml.append('      - name: ' ~ column_name | lower ) %}
    {% do model_yaml.append('        description: "' ~ column_description ~ '"') %}
    {% do model_yaml.append('        data_type: "' ~ column_datatype ~ '"') %}
    {% do model_yaml.append('') %}

    {% if column.fields|length > 0 %}
        {% for child_column in column.fields %}
            {% set model_yaml = generate_column_yaml_descriptions(child_column, model_yaml, column_desc_dict, parent_column_name=column_name) %}
        {% endfor %}
    {% endif %}
    {% do return(model_yaml) %}
{% endmacro %}
