#!/usr/bin/php
<?php
/**
 * Convert PostgreSQL Database Tables and Data into Sqlite
 * @depends on PDO with PGSQL and SQLITE drivers
 * @see https://manuelvanrijn.nl/blog/2012/01/18/convert-postgresql-to-sqlite/
 */

error_reporting((E_ALL|E_STRICT) & ~E_NOTICE);

$opt = _cli_option_parse();

$dbc_source = new SQL($opt['source']);

// $src_schema = _source_schema_load($dbc_source);
// $src_schema = _source_schmea_filter($src_schema, $opt);
// _target_create($dbc_target, $)
// _target_insert($dbc_source, $dbc_target, $xxx);

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
			'SELECT' => '',
			'INSERT' => '',
			'FK' => [],
		];
	}

	// Remap Datatype
	$col_type = $obj['col_type'];
	$col_type = preg_replace('/\(.+\)/', '', $col_type);
	switch ($col_type) {
	case 'bigint':
	case 'integer':
		$obj['col_type'] = 'INTEGER';
		break;
	case 'character':
	case 'character varying':
	case 'date':
	case 'text':
	case 'timestamp with time zone':
		$obj['col_type'] = 'TEXT';
		break;
	case 'json':
	case 'jsonb':
	case 'tsvector':
		$obj['col_type'] = 'BLOB';
		break;
	case 'numeric':
		$obj['col_type'] = 'REAL';
		break;
	default:
		echo "TYPE: {$obj['col_type']}\n";
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


// Load Foreign Keys
$sql = <<<SQL
SELECT
	tc.table_name, kcu.column_name, tc.constraint_name,
	ccu.table_name AS foreign_table_name,
	ccu.column_name AS foreign_column_name
FROM
	information_schema.table_constraints AS tc
	LEFT JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
	LEFT JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY'
ORDER BY 1, 2, 3, 4, 5
SQL;

$res = $dbc_source->query($sql)->fetchAll();
foreach ($res as $rec) {

	$t = $rec['table_name'];
	$c = $rec['column_name'];

	$out_schema[$t]['FK'][$c] = [
		't' => $rec['foreign_table_name'],
		'c' => $rec['foreign_column_name'],
	];

}


// Filter some Out?
if (!empty($opt['filter'])) {
	$del_list = [];
	foreach ($out_schema as $tab_name => $tab_spec) {
		if (!preg_match($opt['filter'], $tab_name)) {
			$del_list[] = $tab_name;
		}
	}
	// var_dump($del_list);
	foreach ($del_list as $x) {
		unset($out_schema[$x]);
	}
}


// Create Output
$dbc_target = new SQL($opt['target']);
// This makes Sqlite "faster" (but less safe)
$dbc_target->query('PRAGMA journal_mode = OFF');
$dbc_target->query('PRAGMA synchronous = OFF');


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

	// Append Foreign Keys
	if (!empty($tab_spec['FK'])) {
		foreach ($tab_spec['FK'] as $col_name => $col_spec) {
			$col_text[] = sprintf('FOREIGN KEY (%s) REFERENCES %s(%s)', $col_name, $col_spec['t'], $col_spec['c']);
		}
	}

	$col_text = implode(",\n  ", $col_text);
	$sel_text = implode(', ', $sel_text);
	$ins_text = implode(', ', $ins_text);

	// Create Table
	$sql_create = "CREATE TABLE $tab_name (\n  $col_text\n);";
	// echo "$sql_create\n";
	$dbc_target->query($sql_create);

	// Modify out_schema with SELECT and INSERT data-helper
	$out_schema[$tab_name]['SELECT'] = $sel_text;
	$out_schema[$tab_name]['INSERT'] = $ins_text;

}


// Import the Data
foreach ($out_schema as $tab_name => $tab_spec) {

	$idx_insert = 0;

	// Prepare insert statement
	$sql_select = sprintf('DECLARE _pg_dump_cursor CURSOR FOR SELECT * FROM ONLY %s', $tab_name);
	$sql_insert = "INSERT INTO $tab_name VALUES ({$tab_spec['INSERT']});";
	echo "INSERT: $sql_insert\n";

	$dbc_source->query('BEGIN');
	$cur_select = $dbc_source->prepare($sql_select);
	$cur_select->execute();

	$res_select = $dbc_source->prepare('FETCH 1000 FROM _pg_dump_cursor');
	$res_insert = $dbc_target->prepare($sql_insert);

	$res_select->execute();
	while ($res_select->rowCount() > 0) {

		printf("INSERT: $idx_insert + %d...\r", $res_select->rowCount());

		// Not sure if these transactions affect the already prepared statement
		// $dbc_target->query('BEGIN');
		foreach ($res_select as $rec_source) {
			$idx_insert++;
			$rec_source = array_values($rec_source);
			$res_insert->execute($rec_source);
		}
		// $dbc_target->query('COMMIT');

		$res_select->execute();

	};

	$dbc_source->query('ROLLBACK');

	echo "\nINSERT: $idx_insert RECORDS\n";
}


/**
 * SQL Helper Class
 */
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

/**
 * Parse Command Line Options
 * @return Array Options
 */
function _cli_option_parse()
{
	$opt = getopt('', [
		'source:',
		'output:',
		'filter:', // Filter to INCLUDE ONLY these Table Names
	]);

	if (empty($opt['source'])) {
		echo "You must provide --source=DSN\n";
		exit(1);
	}

	if (empty($opt['output'])) {
		$opt['target'] = sprintf('sqlite:%s/OUTPUT-%s.sqlite', __DIR__, date('YmdHis'));
		printf("WARN: Using Default Output: %s\n", basename($opt['target']));
	}

	// Check Filter
	if (!empty($opt['filter'])) {
		$x = preg_match($opt['filter'], 'TEST STRING NEVER MATCH');
		$e = preg_last_error();
		if (!empty($e)) {
			echo "Your Regular Expression in --filter is not valid\n";
			exit(1);
		}
	}

	return $opt;
}
