<?php

/***
 * Configuration 
 */
$servername = "localhost";
$username = "root";
$password = "";
$dbname = "temp_db";

$tablename = 'philippines_barangays'; //name of table to be created
$csvFilename = 'PSGC-1Q-2025-Publication-Datafile.csv'; //csv file from PSGC

/****END Config here***/



$spinnerIndex = 0;
function printSpinner() {
	global $spinnerIndex;

	$c = ['-', '\\', '|', '/'];
	echo chr(8);
	echo $c[$spinnerIndex];
	$spinnerIndex++;
	if ($spinnerIndex == 3) {
		$spinnerIndex = 0;
	}
}

// Create connection
$conn = new mysqli($servername, $username, $password, $dbname);
// Check connection
if ($conn->connect_error) {
  die("Connection failed: " . $conn->connect_error);
}

$csv = new SplFileObject(__DIR__. "/{$csvFilename}");
$csv->setFlags(SplFileObject::READ_CSV | SplFileObject::READ_AHEAD | SplFileObject::SKIP_EMPTY | SplFileObject::DROP_NEW_LINE);

$startIndex =   1; //skip header
$sqlInsertChunkSize = 1000;




// sql to create table
$sql = "CREATE TABLE `{$tablename}` (
  `id` int(11) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `brgy_code` varchar(20) DEFAULT NULL,
  `city_mun_code` varchar(10) DEFAULT NULL,
  `prov_code` varchar(10) DEFAULT NULL,
  `region_code` varchar(10) DEFAULT NULL,
  `brgy_name` text,
  `city_mun_name` varchar(255) DEFAULT NULL,
  `prov_name` varchar(255) DEFAULT NULL,
  `region_name` varchar(255) DEFAULT NULL,
  `island_group` varchar(20) DEFAULT NULL
) ";

if ($conn->query($sql) === TRUE) {
  	echo "Table {$tablename} created successfully\n";
} else {
  	die("Error creating table: " . $conn->error);
}


// Define column indices for easier reference
// PSGC Code, Name, and Geographic Level
// change this if the CSV columns are different
$colInfo = [
	'code' => 2, // correspondence Code
	'name' => 1, // Name
	'geo_level' => 3 // Geographic Level
];


echo "writing barangays\n";
echo "***";
while( !$csv->eof() ) {
    foreach(new LimitIterator($csv, $startIndex, $sqlInsertChunkSize) as $index => $line) {
    	if ($line[$colInfo['geo_level']] == 'Bgy') {
    		printSpinner();

			if (!$line[$colInfo['code']]) {
				continue; //skip if no code
			}

			$name = $line[$colInfo['name']];
			$correspondenceCode = str_pad($line[$colInfo['code']], 9, '0', STR_PAD_LEFT);

			$provCode = substr($correspondenceCode, 0, 4);

    		$sql = "INSERT INTO `{$tablename}` (`brgy_code`, `brgy_name`, `prov_code`) VALUES (?, ?, ?)";
    		//$sql = "INSERT INTO `{$tablename}` (`brgy_code`, `brgy_name`) VALUES ('{$psgcCode}', '{$name}')";
			$statement = $conn->prepare($sql);
			$statement->bind_param('sss', $correspondenceCode, $name, $provCode);
			$statement->execute();
			$statement->close();
    		//$conn->query($sql);
    	}
    }

    $startIndex += $sqlInsertChunkSize;
}

//reset csv
$csv->rewind();
$startIndex =   1; //skip header



echo "\n";
echo "writing city, municipality, province, region\n";
echo "***";
while( !$csv->eof() ) {
    foreach(new LimitIterator($csv, $startIndex, $sqlInsertChunkSize) as $index => $line) {
    	
    	printSpinner();

		$name = $line[$colInfo['name']];
		$correspondenceCode = str_pad($line[$colInfo['code']], 9, '0', STR_PAD_LEFT);
		

    	switch($line[$colInfo['geo_level']]) {
    		case 'City':
    		case 'Mun':
    		case 'SubMun':
    			$code = substr($correspondenceCode, 0, 6);
	    		$cityname = str_replace('City of ', '', $name);
	    		$sql = "UPDATE `{$tablename}` SET `city_mun_code`= ?, `city_mun_name`= ? WHERE brgy_code LIKE ?";
	    		//$sql = "UPDATE `{$tablename}` SET `city_mun_code`='{$code}', `city_mun_name`='{$cityname}' WHERE brgy_code LIKE '{$code}%'";
				$likeCode = $code.'%';
				$statement = $conn->prepare($sql);
				$statement->bind_param('sss', $code, $cityname, $likeCode);
				$statement->execute();
				$statement->close();
	    		//$conn->query($sql);
    		break;

    		case 'Prov':
    		case 'Dist':
    			$code = substr($correspondenceCode, 0, 4);
	    		$sql = "UPDATE `{$tablename}` SET `prov_code`= ?, `prov_name`= ? WHERE brgy_code LIKE ?";
	    		//$sql = "UPDATE `{$tablename}` SET `prov_code`='{$code}', `prov_name`='{$name}' WHERE brgy_code LIKE '{$code}%'";
	    		//$conn->query($sql);
				$likeCode = $code.'%';
				$statement = $conn->prepare($sql);
				$statement->bind_param('sss', $code, $name, $likeCode);
				$statement->execute();
				$statement->close();
    		break;

    		case 'Reg':
    			$code = substr($correspondenceCode, 0, 2);
	    		$sql = "UPDATE `{$tablename}` SET `region_code`= ?, `region_name`= ? WHERE brgy_code LIKE ?";
	    		//$sql = "UPDATE `{$tablename}` SET `region_code`='{$code}', `region_name`='{$name}' WHERE brgy_code LIKE '{$code}%'";
	    		//$conn->query($sql);
				$likeCode = $code.'%';
				$statement = $conn->prepare($sql);
				$statement->bind_param('sss', $code, $name, $likeCode);
				$statement->execute();
				$statement->close();
    		break;

    	}
    }

    $startIndex += $sqlInsertChunkSize;
}

echo "\nwriting island groups\n";

$sql = "UPDATE `{$tablename}` SET `island_group`='Luzon' WHERE `region_code` IN ('01','02', '03', '04', '05', '13', '14', '17') ";
$conn->query($sql);

$sql = "UPDATE `{$tablename}` SET `island_group`='Visayas' WHERE `region_code` IN ('06','07', '08') ";
$conn->query($sql);

$sql = "UPDATE `{$tablename}` SET `island_group`='Mindanao' WHERE `region_code` IN ('09','10', '11', '12', '15', '16') ";
$conn->query($sql);

$conn->close();