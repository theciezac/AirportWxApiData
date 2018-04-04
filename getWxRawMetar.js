const request = require('request'),
fs = require('fs'),
parseMETAR = require('metar'),
moment = require('moment');

const { Client } = require('pg'); 
const client = new Client({
    user: "postgres",
    host: "localhost",
    database: "database",
    password: "password",
    port: "5432"
});

client.connect();

const metarColumnSubStrTemplate = "INSERT INTO \"METAR\"(";
const valuesSubStrTemplate = ") VALUES (";
const queryEndTemplate = ") RETURNING id;";

const metarRvrColumnSubStrTemplate = "INSERT INTO \"METAR_RVR\"(";
const metarWeatherColumnSubStrTemplate = "INSERT INTO \"METAR_conditions\"(";
const metarCloudsColumnSubStrTemplate = "INSERT INTO \"METAR_clouds\"(";

var apiKey;

function getMETARData() {
    request.get("https://api.checkwx.com/metar/WSSS", {
        headers: {
            "Accept": "application/json",
            "X-API-Key": apiKey
        }
    }, function (error, response, body) {
        if (error) {
            console.log("Error: " + error);
        } else {
            responseBodyJson = JSON.parse(response.body);
            console.log("Result: " + responseBodyJson.data[0]);
            //console.log("Body: \n" + body);

            var queryStr = "SELECT * FROM \"METAR\" ORDER BY observed DESC LIMIT 1;";

            var metarStr;

            client.query(queryStr, function (err, res) {
                if (err) {
                    throw err;
                }
                var toInsertNewRecToDb = false;

                var timestamp = new Date();

                if (res.rowCount > 0) {
                    var timestamp_issued = (res.rows[0])["observed"];
            
                    var metarStr = (res.rows[0])["raw_text"];
                    if (metarStr.slice(-1) === "=") {
                        metarStr = metarStr.slice(0, -1);
                    }
                    //console.log(metarStr);
    
                    if (responseBodyJson.data[0] === metarStr) {
                        console.log("[" + timestamp.toISOString() + "]: Same METAR record. No need to update.");
                    } else {
                        console.log("[" + timestamp.toISOString() + "]: New METAR from previous record.");
                        toInsertNewRecToDb = true;
                    }
                } else {
                    console.log("[" + timestamp.toISOString() + "]: No METAR records in table.");
                    toInsertNewRecToDb = true;
                }

                if (toInsertNewRecToDb === true) {
                    console.log("To insert new METAR record into database.");
                    insertMetarIntoDb(responseBodyJson.data[0]);
                }
            });
        }
    });
}

function insertMetarIntoDb(metarStr) {
    metarStr = metarStr;
    var metarObj = parseMETAR(metarStr);

    var columnSubStr = metarColumnSubStrTemplate;
    var valuesSubStr = valuesSubStrTemplate;

    var raw_text = metarStr;
    columnSubStr += "raw_text, ";
    valuesSubStr += ("'" + raw_text + "', ");

    var station = metarObj["station"];
    columnSubStr += "icao, ";
    valuesSubStr += ("'" + station + "', ");

    var observed = metarObj["time"];
    columnSubStr += "observed, ";
    valuesSubStr += ("'" + observed.toISOString() + "', ");

    var auto = metarObj["auto"];
    columnSubStr += "auto, ";
    valuesSubStr += ("'" + auto + "', ");

    var cavok = metarObj["cavok"];
    columnSubStr += "cavok";
    valuesSubStr += ("'" + cavok + "'");

    if ("wind" in metarObj && (metarObj["wind"] != null)) {
        if ("speed" in metarObj["wind"] && (metarObj["wind"]["speed"] != null)) {
            var wind_speed = metarObj["wind"]["speed"];
            columnSubStr += ", wind_speed_kts";
            valuesSubStr += (", " + wind_speed);    
        }

        if ("gust" in metarObj["wind"] && (metarObj["wind"]["gust"] != null)) {
            var wind_gust = metarObj["wind"]["gust"];
            columnSubStr += ", wind_gust_kts";
            valuesSubStr += (", " + wind_gust);
        }
        
        if ("direction" in metarObj["wind"] && (metarObj["wind"]["direction"] != null)) {
            var wind_direction = metarObj["wind"]["direction"];
            columnSubStr += ", wind_degrees";
            valuesSubStr += (", " + wind_direction);
        }

        if ("vrb" in metarObj["wind"] && (metarObj["wind"]["vrb"] != null)) {
            var wind_vrb = metarObj["wind"]["vrb"];
            columnSubStr += ", wind_variation";
            valuesSubStr += (", '" + wind_vrb + "'");
        }

        if ("variation" in metarObj["wind"] && (metarObj["wind"]["variation"] != null)) {
            var wind_variation_min = metarObj["wind"]["variation"]["min"];
            columnSubStr += ", wind_variation_min_degrees";
            valuesSubStr += (", " + wind_variation_min);

            var wind_variation_max = metarObj["wind"]["variation"]["max"];
            columnSubStr += ", wind_variation_max_degrees";
            valuesSubStr += (", " + wind_variation_max);
        }
    }

    if ("visibility" in metarObj && !isNaN(metarObj["visibility"])) {
        var visibility = metarObj["visibility"];
        columnSubStr += ", visibility_meters";
        valuesSubStr += (", " + visibility);
    }

    if ("temperature" in metarObj && !isNaN(metarObj["temperature"])) {
        var temperature = metarObj["temperature"];
        columnSubStr += ", temperature_celsius";
        valuesSubStr += (", " + temperature);
    }

    if ("dewpoint" in metarObj && !isNaN(metarObj["dewpoint"])) {
        var dewpoint = metarObj["dewpoint"];
        columnSubStr += ", dewpoint_celsius";
        valuesSubStr += (", " + dewpoint);
    }

    if ("altimeterInHpa" in metarObj && !isNaN(metarObj["altimeterInHpa"])) {
        var barometer_mb = metarObj["altimeterInHpa"];
        columnSubStr += ", barometer_mb";
        valuesSubStr += (", " + barometer_mb);
    }

    var fullQueryStr = columnSubStr + valuesSubStr + queryEndTemplate;
    client.query(fullQueryStr, (err, res) => {
        console.log("Insert query statement: ");
        console.log(fullQueryStr);
        if (err) {
            console.log("Error: ");
            console.log(err);
        }

        if (res.rows.length) {
            console.log("Added into METAR table. id: " + (res.rows[0])["id"]);
        }

        var historicalmetar_id = (res.rows[0])["id"];

        if ("rvr" in metarObj && (metarObj["rvr"] != null)) {
            var rvrArr = metarObj["rvr"];

            for (var r in rvrArr) {
                var rvrColumnSubStr = metarRvrColumnSubStrTemplate;
                var rvrValuesSubStr = valuesSubStrTemplate;

                rvrColumnSubStr += "\"METAR_id\"";
                rvrValuesSubStr += historicalmetar_id;

                if ("runway" in rvrArr[r] && rvrArr[r]["runway"] != null) {
                    var runway = rvrArr[r]["runway"];
                    rvrColumnSubStr += ", runway";
                    rvrValuesSubStr += (", '" + runway + "'");
                }

                if ("direction" in rvrArr[r] && rvrArr[r]["direction"] != null) {
                    var direction = rvrArr[r]["direction"];
                    rvrColumnSubStr += ", direction";
                    rvrValuesSubStr += (", '" + direction + "'");
                }

                if ("minIndicator" in rvrArr[r] && rvrArr[r]["minIndicator"] != null) {
                    var minIndicator = rvrArr[r]["minIndicator"];
                    rvrColumnSubStr += ", min_indicator";
                    rvrValuesSubStr += (", '" + minIndicator + "'");
                }

                if ("minValue" in rvrArr[r] && rvrArr[r]["minValue"] != null) {
                    var minValue = rvrArr[r]["minValue"];
                    rvrColumnSubStr += ", min_value";
                    rvrValuesSubStr += (", " + minValue);
                }

                if ("variableIndicator" in rvrArr[r] && rvrArr[r]["variableIndicator"] != null) {
                    var variableIndicator = rvrArr[r]["variableIndicator"];
                    rvrColumnSubStr += ", variable_indicator";
                    rvrValuesSubStr += (", '" + variableIndicator + "'");
                }

                if ("maxIndicator" in rvrArr[r] && rvrArr[r]["maxIndicator"] != null) {
                    var maxIndicator = rvrArr[r]["maxIndicator"];
                    rvrColumnSubStr += ", max_indicator";
                    rvrValuesSubStr += (", '" + maxIndicator + "'");
                }

                if ("maxValue" in rvrArr[r] && rvrArr[r]["maxValue"] != null) {
                    var maxValue = rvrArr[r]["maxValue"];
                    rvrColumnSubStr += ", max_value";
                    rvrValuesSubStr += (", " + maxValue);
                }

                if ("trend" in rvrArr[r] && rvrArr[r]["trend"] != null) {
                    var trend = rvrArr[r]["trend"];
                    rvrColumnSubStr += ", trend";
                    rvrValuesSubStr += (", '" + trend  + "'");
                }

                var fullRvrQuery = rvrColumnSubStr + rvrValuesSubStr + queryEndTemplate;
                console.log(fullRvrQuery);

                client.query(fullRvrQuery, (err, res) => {
                    if (res.rows.length > 0) {
                        console.log("Inserted record into METAR_RVR table. id: " + (res.rows[0])["id"]);
                    }
                });
            }
        }

        if ("clouds" in metarObj && (metarObj["clouds"] != null)) {
            var cloudsArr = metarObj["clouds"]
            for (var i in cloudsArr) {
                var cloudsColumnSubStr = metarCloudsColumnSubStrTemplate;
                var cloudsValuesSubStr = valuesSubStrTemplate;

                cloudsColumnSubStr += "\"METAR_id\"";
                cloudsValuesSubStr += historicalmetar_id;

                if ("abbreviation" in cloudsArr[i]) {
                    var abbreviation = cloudsArr[i]["abbreviation"];
                    cloudsColumnSubStr += ", code";
                    cloudsValuesSubStr += (", '" + abbreviation + "'");
                }

                if ("meaning" in cloudsArr[i]) {
                    var meaning = cloudsArr[i]["meaning"];
                    cloudsColumnSubStr += ", text";
                    cloudsValuesSubStr += (", '" + meaning + "'");
                }

                if ("altitude" in cloudsArr[i]) {
                    var altitude = cloudsArr[i]["altitude"];
                    cloudsColumnSubStr += ", base_meters_agl";
                    cloudsValuesSubStr += (", " + altitude);
                }

                if ("cumulonimbus" in cloudsArr[i]) {
                    var cumulonimbus = cloudsArr[i]["cumulonimbus"];
                    cloudsColumnSubStr += ", cumulonimbus";
                    cloudsValuesSubStr += (", '" + cumulonimbus + "'");
                }

                if ("toweringCumulus" in cloudsArr[i]) {
                    var toweringCumulus = cloudsArr[i]["toweringCumulus"];
                    cloudsColumnSubStr += ", towering_cumulus";
                    cloudsValuesSubStr += (", '" + toweringCumulus + "'");
                }

                var fullCloudsQuery = cloudsColumnSubStr + cloudsValuesSubStr + queryEndTemplate;

                client.query(fullCloudsQuery, (err, res) => {
                    if (res.rows.length > 0) {
                        console.log("Inserted record into METAR_clouds table. id: " + (res.rows[0])["id"]);
                    }
                });
            }
        }

        if ("weather" in metarObj && (metarObj["weather"] != null)) {
            var weatherArr = metarObj["weather"];

            for (var j in weatherArr) {
                var weatherColumnSubStr = metarWeatherColumnSubStrTemplate;
                var weatherValuesSubStr = valuesSubStrTemplate;

                weatherColumnSubStr += "\"METAR_id\"";
                weatherValuesSubStr += historicalmetar_id;
                if ("abbreviation" in weatherArr[j]) {
                    var abbreviation = weatherArr[j]["abbreviation"];
                    weatherColumnSubStr += ", code";
                    weatherValuesSubStr += (", '" + abbreviation + "'");
                }

                if ("meaning" in weatherArr[j]) {
                    var meaning = weatherArr[j]["meaning"];
                    weatherColumnSubStr += ", text";
                    weatherValuesSubStr += (", '" + meaning + "'");
                }

                var fullWeatherQuery = weatherColumnSubStr + weatherValuesSubStr + queryEndTemplate;

                client.query(fullWeatherQuery, (err, res) => {
                    if (res.rows.length > 0) {
                        console.log("Inserted record into METAR_conditions table. id: " + (res.rows[0])["id"]);
                    }
                });
            }
        };
    });
}

fs.readFile("apikey.ini", function (err, data) {
    if (err) {
        throw err;
        process.exit();
    }
    apiKey = data;
    getMETARData();
    setInterval(getMETARData, 300000); // 300000 milliseconds = 5 minutes
});