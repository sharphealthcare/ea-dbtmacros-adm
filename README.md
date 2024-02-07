
### DBT Snowflake Documentation Macro

Run the macro via the following command and then copy it into your schema file.  You can run it for a model or a folder.  Be careful as the output goes to the commandline and can't be downloaded.

To use the generate_model_documentation macro: 
1. Update packages.yml with the following
  - git: "https://github.com/sharphealthcare/ea-dbtmacros-adm.git"
    revision: 1.1.6
  - package: dbt-labs/codegen
    version: 0.10.0    
2. dbt deps
3. From the command line:
   for a model folder: 
   dbt run-operation generate_model_documentation --args '{ "model_folder": "<<YOUR MODEL FOLDER>>" }'
   e.g. 
   dbt run-operation generate_model_documentation --args '{ "model_folder": "surgery" }'
   for a single model: 
   dbt run-operation generate_model_documentation --args '{ "my_unique_model": "<<YOUR UNIQUE MODEL>>" }'
   e.g. 
   dbt run-operation generate_model_documentation --args '{ "my_unique_model": "sg_vw_surgical_supply_utilization" }'
4. Expand System Logs to see generated yaml.
5. There is still a feature that needs to be re-visited for retrieving database descriptions and data types.
