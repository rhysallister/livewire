/*		Returns the version of livewire 		*/

CREATE OR REPLACE FUNCTION lw_version(
  OUT lw_version text
  ) AS
  
$lw_version$

SELECT 'Livewire 0.2. Build '

$lw_version$ LANGUAGE sql;

COMMENT ON FUNCTION lw_version(IN lw_schema text) IS
  'Returns the verison of livewire.';
