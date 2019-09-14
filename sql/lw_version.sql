/*		Returns the version of livewire 		*/

CREATE OR REPLACE FUNCTION lw_version(
  OUT lw_version text
  ) AS
  
$lw_version$

SELECT 'LiveWire 0.4 Build '

$lw_version$ LANGUAGE sql;

COMMENT ON FUNCTION lw_version() IS
  'Returns the version of livewire.';
