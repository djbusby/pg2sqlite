#!/usr/bin/php
<?php
/**
 * Convert PostgreSQL Database Tables and Data into Sqlite
 * @depends on PDO with PGSQL and SQLITE drivers
 * @see https://manuelvanrijn.nl/blog/2012/01/18/convert-postgresql-to-sqlite/
 */

$dsn_source = $argv[1];
$dsn_source = $argv[1];
$dsn_target = $argv[2];

$dbc_source = new SQL($dsn_source);
$dbc_target = new SQL($dsn_target);


// Get Schema From Source
$sql = <<<SQL
SELECT DISTINCT ON (col_name) c.relname || '.' || a.attname AS col_name
, pg_catalog.format_type(a.atttypid, a.atttypmod) AS col_type
, NOT a.attnotnull AS col_null
, a.attnum AS col_sort
, (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) AS col_default
FROM pg_catalog.pg_class c
 JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
 LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid
 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r')
 AND n.nspname = 'public'
 AND pg_catalog.pg_table_is_visible(c.oid)
 AND a.attnum > 0 AND NOT a.attisdropped
SQL;

$res_schema = $dbc_source->query($sql)->fetchAll();

$out_schema = [];
foreach ($res_schema as $obj) {

	$tab = strtok($obj['col_name'], '.');
	$col = strtok('');

	if (empty($out_schema[$tab])) {
		$out_schema[$tab] = [
			'name' => $tab,
			'column_list' => [],
		];
	}

	// Remap Datatype
	switch ($obj['col_type']) {
	case 'integer':
		$obj['col_type'] = 'INTEGER';
		break;
	case 'json':
	case 'jsonb':
		$obj['col_type'] = 'BLOB';
		break;
	default:
		$obj['col_type'] = 'TEXT';
		break;
	}

	$out_schema[$tab]['column_list'][$col] = [
		'name' => $col,
		'type' => $obj['col_type'],
		'null' => $obj['col_null'],
		'sort' => $obj['col_sort'],
		'default_value' => $obj['col_default'],
	];

}
ksort($out_schema);

// Spin Discovered Schema
foreach ($out_schema as $tab_name => $tab_spec) {

	uasort($tab_spec['column_list'], function($a, $b) {
		return ($a['sort'] > $b['sort']);
	});

	// Column Constructor
	$col_text = [];
	$sel_text = [];
	$ins_text = [];
	foreach ($tab_spec['column_list'] as $col_name => $col_spec) {
		$col_text[] = "$col_name {$col_spec['type']}";
		$sel_text[] = $col_name;
		$ins_text[] = '?'; // @todo Named Params
	}
	$col_text = implode(",\n  ", $col_text);
	$sel_text = implode(', ', $sel_text);
	$ins_text = implode(', ', $ins_text);

	// Create Table
	$sql_create = "CREATE TABLE $tab_name ($col_text)";
	echo "$sql_create\n";
	//$dbc_target->query($sql_create);

	// Prepare insert statement
	$sql_target = "INSERT INTO $tab_name VALUES ($ins_text)";
	$res_insert = $dbc_target->prepare($sql_target);

	// Copy Data
	// @todo use the CURSOR Format
	do {

		$add = 0;
		$lim = 100000;

		// Get all source data
		// $sql_source = "SELECT $sel_text FROM $tab_name ORDER BY 1 OFFSET $idx LIMIT $lim";
		$sql_source = "SELECT $sel_text FROM $tab_name ORDER BY 1 OFFSET $idx";
		echo "$sql_source\n";

		$res_source = $dbc_source->query($sql_source);

		// $dbc_target->query('BEGIN');

		while ($rec_source = $res_source->fetch(\PDO::FETCH_NUM)) {

			$add++;
			$idx++;

			$res_insert->execute($rec_source);

		}

		// $dbc_target->query('COMMIT');

	} while ($add > 0);

}

// SQL Helper
class SQL extends \PDO
{
	function __construct($dsn=null, $user=null, $pass=null, $opts=null)
	{
		parent::__construct($dsn, $user, $pass, $opts);
		$this->setAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE, \PDO::FETCH_ASSOC);
		$this->setAttribute(\PDO::ATTR_CASE, \PDO::CASE_NATURAL);
		$this->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
		$this->setAttribute(\PDO::ATTR_ORACLE_NULLS, \PDO::NULL_EMPTY_STRING);

	}
}
