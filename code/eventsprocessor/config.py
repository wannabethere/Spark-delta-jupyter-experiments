import os
import json

class Config(JSONSerializer):
    def __init__(self,path=None,owner, json):
       schemas =  _process_schemas_loc(path, owner)
       self._pk = columnname
       self._secondary = secondarycolumn
       self._timestamp = timestampcolumn
       
        
            
def _process_schemas_loc(schemaloc, owner):
	path = schemaloc+owner
	path_list = [dirpath[len(path):]+":"+filename for dirpath, _, filenames in os.walk(path) for filename in filenames if not filename.endswith('.DS_Store')]
	schemas = []
	for item in path_list:
		k = item.split(':')
		schemas.append({'location': schemaloc,'dirpath':owner,'type':k[0],'file':k[1] })
	return schemas
    
    
if __name__ == "__main__":
    # execute only if run as a script
    _process_schemas_loc()    
    
    