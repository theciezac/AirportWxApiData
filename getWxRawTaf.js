const request = require('request'),
fs = require('fs'),
parseTAF = require('./TAF.js'),
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

const tafColumnSubStrTemplate = "INSERT INTO \"TAF\"(";
const valuesSubStrTemplate = ") VALUES (";
const queryEndTemplate = ") RETURNING id;";

const tafForecastColumnSubStrTemplate = "INSERT INTO \"TAF_forecast\"(";
const tafCloudsColumnSubStrTemplate = "INSERT INTO \"TAF_forecast_clouds\"(";
const tafConditionsColumnSubStrTemplate = "INSERT INTO \"TAF_forecast_conditions\"(";

var apiKey;

function getTAFData() {
    request.get("https://api.checkwx.com/taf/WSSS", {
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

            var queryStr = "SELECT * FROM \"TAF\" ORDER BY timestamp_issued DESC LIMIT 1;";

            var tafStr;

            client.query(queryStr, function (err, res) {
                if (err) {
                    throw err;
                }
                var toInsertNewRecToDb = false;

                var timestamp = new Date();

                if (res.rowCount > 0) {
            
                    var tafStr = (res.rows[0])["raw_text"];
                    if (tafStr.slice(-1) === "=") {
                        tafStr = tafStr.slice(0, -1);
                    }
                    //console.log(tafStr);
    
                    if (responseBodyJson.data[0] === tafStr) {
                        console.log("[" + timestamp.toISOString() + "]: Same TAF record. No need to update.");
                    } else {
                        console.log("[" + timestamp.toISOString() + "]: New TAF from previous record.");
                        toInsertNewRecToDb = true;
                    }
                } else {
                    console.log("[" + timestamp.toISOString() + "]: No TAF records in table.");
                    toInsertNewRecToDb = true;
                }

                if (toInsertNewRecToDb === true) {
                    console.log("To insert new TAF record into database.");
                    insertTafIntoDb(responseBodyJson.data[0]);
                }
            });
        }
    });
}

function insertTafIntoDb(tafStr) {
    var tafObj = parseTAF(tafStr);

    var columnSubStr = tafColumnSubStrTemplate;
    var valuesSubStr = valuesSubStrTemplate;

    var raw_text = tafStr;
    columnSubStr += "raw_text, ";
    valuesSubStr += ("'" + raw_text + "', ");

    var icao = tafObj["station"];
    columnSubStr += "icao, ";
    valuesSubStr += ("'" + icao + "', ");

    var time = (new Date(tafObj["time"])).toISOString();
    columnSubStr += "timestamp_issued, ";
    valuesSubStr += ("'" + time + "', ");
    
    columnSubStr += "timestamp_bulletin";
    valuesSubStr += ("'" + time + "'");

    var validFrom = (new Date(tafObj["validFrom"])).toISOString();
    columnSubStr += ", timestamp_valid_from";
    valuesSubStr += (", '" + validFrom + "'");

    var validTo = (new Date(tafObj["validTo"])).toISOString();
    columnSubStr += ", timestamp_valid_to";
    valuesSubStr += (", '" + validTo + "'");

    var fullQueryStr = columnSubStr + valuesSubStr + queryEndTemplate;
    console.log(fullQueryStr);
    client.query(fullQueryStr, (err, res) => {
        console.log("Insert query statement: ");
        console.log(fullQueryStr);
        if (err) {
            console.log("Error: ");
            console.log(err);
        }

        if (res.rows.length) {
            console.log("Added into TAF table. id: " + (res.rows[0])["id"]);
        }

        var taf_id = (res.rows[0])["id"];

        var forecastColumnSubStr = tafForecastColumnSubStrTemplate;
        var forecastValuesSubStr = valuesSubStrTemplate;
        
        forecastColumnSubStr += "\"TAF_id\"";
        forecastValuesSubStr += taf_id;

        var timestamp_forecast_from = (new Date(tafObj["validFrom"])).toISOString();
        forecastColumnSubStr += ", timestamp_forecast_from";
        forecastValuesSubStr += (", '" + timestamp_forecast_from + "'");

        var timestamp_forecast_to = (new Date(tafObj["validTo"])).toISOString();
        forecastColumnSubStr += ", timestamp_forecast_to";
        forecastValuesSubStr += (", '" + timestamp_forecast_to + "'");

        var change_indicator = "From"; // "Temporary" if "TEMPO", else "From"
        forecastColumnSubStr += ", change_indicator";
        forecastValuesSubStr += ", '" + change_indicator + "'";

        if ("visibility" in tafObj && (tafObj["visibility"] != null)) {
            var visibility = tafObj["visibility"]; 
            forecastColumnSubStr += ", visibility_meters";
            forecastValuesSubStr += (", '" + visibility + "'");
        }

        if ("wind" in tafObj && (tafObj["wind"] != null)) {
            if ("direction" in tafObj["wind"] && (tafObj["wind"]["direction"] != null)) {
                var wind_direction = tafObj["wind"]["direction"];
                forecastColumnSubStr += ", wind_degrees";
                forecastValuesSubStr += (", '" + wind_direction + "'");
            }

            if ("speed" in tafObj["wind"] && (tafObj["wind"]["speed"] != null)) {
                var wind_speed = tafObj["wind"]["speed"];
                forecastColumnSubStr += ", wind_speed_kts";
                forecastValuesSubStr += (", " + wind_speed);    
            }

            if ("gust" in tafObj["wind"] && (tafObj["wind"]["gust"] != null)) {
                var wind_gust = tafObj["wind"]["gust"];
                forecastColumnSubStr += ", wind_gust_kts";
                forecastValuesSubStr += (", " + wind_gust);
            }
        }

        if ("probability" in tafObj && (tafObj["wind" != null])) {
            var probability = tafObj["probability"];
            forecastColumnSubStr += ", probability";
            forecastValuesSubStr += (", " + probability);
        }

        var forecastQueryStr = forecastColumnSubStr + forecastValuesSubStr + queryEndTemplate;
        client.query(forecastQueryStr, (err, res) => {
            console.log("Insert query statement: ");
            console.log(forecastQueryStr);
            if (err) {
                console.log("Error: ");
                console.log(err);
            }

            if (res.rows.length) {
                console.log("Added into TAF_forecast table. id: " + (res.rows[0])["id"]);
            }

            var taf_forecast_id = (res.rows[0])["id"];

            if ("clouds" in tafObj && (tafObj["clouds"] != null)) {
                var cloudsArr = tafObj["clouds"];
                for (var i in cloudsArr) {
                    var cloudsColumnSubStr = tafCloudsColumnSubStrTemplate;
                    var cloudsValuesSubStr = valuesSubStrTemplate;

                    cloudsColumnSubStr += "\"TAF_forecast_id\"";
                    cloudsValuesSubStr += taf_forecast_id;

                    if ("abbreviation" in cloudsArr[i]) {
                        var code = cloudsArr[i]["abbreviation"];
                        cloudsColumnSubStr += ", code";
                        cloudsValuesSubStr += (", '" + code + "'");
                    }

                    if ("meaning" in cloudsArr[i]) {
                        var text = cloudsArr[i]["meaning"];
                        cloudsColumnSubStr += ", text";
                        cloudsValuesSubStr += (", '" + text + "'");
                    }

                    if ("altitude" in cloudsArr[i]) {
                        var base_feet_agl = cloudsArr[i]["altitude"];
                        cloudsColumnSubStr += ", base_feet_agl";
                        cloudsValuesSubStr += (", " + base_feet_agl);
                    }

                    var fullCloudsQuery = cloudsColumnSubStr + cloudsValuesSubStr + queryEndTemplate;

                    client.query(fullCloudsQuery, (err, res) => {
                        if (res.rows.length > 0) {
                            console.log("Inserted record into TAF_forecast_clouds table. id: " + (res.rows[0])["id"]);
                        }
                    });
                }
            }

            if ("weather" in tafObj && (tafObj["weather"] != null)) {
                var conditionsArr = tafObj["weather"];
                for (var j in conditionsArr) {
                    var conditionsColumnSubStr = tafConditionsColumnSubStrTemplate;
                    var conditionsValuesSubStr = valuesSubStrTemplate;
    
                    conditionsColumnSubStr += "\"TAF_forecast_id\"";
                    conditionsValuesSubStr += taf_forecast_id;
    
                    if ("abbreviation" in conditionsArr[j]) {
                        var code = conditionsArr[j]["abbreviation"];
                        conditionsColumnSubStr += ", code";
                        conditionsValuesSubStr += (", '" + code + "'");
                    }
    
                    if ("meaning" in conditionsArr[j]) {
                        var text = conditionsArr[j]["meaning"];
                        conditionsColumnSubStr += ", text";
                        conditionsValuesSubStr += (", '" + text + "'");
                    }

                    var fullConditionsQuery = conditionsColumnSubStr + conditionsValuesSubStr + queryEndTemplate;

                    client.query(fullConditionsQuery, (err, res) => {
                        if (res.rows.length > 0) {
                            console.log("Inserted record into TAF_forecast_conditions table. id: " + (res.rows[0])["id"]);
                        }
                    });
                }
            }
            
            if ("tempo" in tafObj && tafObj["tempo"] != null) {
                var tempoArr = tafObj["tempo"];
                for (var p in tempoArr) {
                    var tempForecastColumnSubStr = tafForecastColumnSubStrTemplate;
                    var tempForecastValuesSubStr = valuesSubStrTemplate;

                    tempForecastColumnSubStr += "\"TAF_id\"";
                    tempForecastValuesSubStr += taf_id;

                    var temp_timestamp_forecast_from = (new Date(tempoArr[p]["validFrom"])).toISOString();
                    tempForecastColumnSubStr += ", timestamp_forecast_from";
                    tempForecastValuesSubStr += (", '" + temp_timestamp_forecast_from + "'");

                    var temp_timestamp_forecast_to = (new Date(tempoArr[p]["validTo"])).toISOString();
                    tempForecastColumnSubStr += ", timestamp_forecast_to";
                    tempForecastValuesSubStr += (", '" + temp_timestamp_forecast_to + "'");

                    var temp_change_indicator = "Temporary";
                    tempForecastColumnSubStr += ", change_indicator";
                    tempForecastValuesSubStr += (", '" + temp_change_indicator + "'");

                    if ("visibility" in tempoArr[p] && (tempoArr[p]["visibility"] != null)) {
                        var temp_visibility = tempoArr[p]["visibility"];
                        tempForecastColumnSubStr += ", visibility_meters";
                        tempForecastValuesSubStr += ", '" + temp_visibility + "'";
                    }

                    if ("wind" in tempoArr[p] && (tempoArr[p]["wind"] != null)) {
                        if ("direction" in tempoArr[p]["wind"] && (tempoArr[p]["wind"]["direction"] != null)) {
                            var temp_wind_direction = tempoArr[p]["wind"]["direction"];
                            tempForecastColumnSubStr += ", wind_degrees";
                            tempForecastValuesSubStr += (", '" + temp_wind_direction + "'");
                        }

                        if ("speed" in tempoArr[p]["wind"] && (tempoArr[p]["wind"]["speed"] != null)) {
                            var temp_wind_speed = tempoArr[p]["wind"]["speed"];
                            tempForecastColumnSubStr += ", wind_speed_kts";
                            tempForecastValuesSubStr += (", " + temp_wind_speed);
                        }

                        if ("gust" in tempoArr[p]["wind"] && (tempoArr[p]["wind"]["gust"] != null)) {
                            var temp_wind_gust = tempoArr[p]["wind"]["gust"];
                            tempForecastColumnSubStr += ", wind_gust_kts";
                            tempForecastValuesSubStr += (", " + temp_wind_gust);
                        }
                    }

                    if ("probability" in tempoArr[p] && (tempoArr[p]["probability"] != null)) {
                        var temp_probability = tempoArr[p]["probability"];
                        tempForecastColumnSubStr += ", probability";
                        tempForecastValuesSubStr += (", " + temp_probability);
                    }
                }

                var tempForecastQueryStr = tempForecastColumnSubStr + tempForecastValuesSubStr + queryEndTemplate;
                client.query(tempForecastQueryStr, (err, res) => {
                    console.log("Insert query statement: ");
                    console.log(tempForecastQueryStr);
                    if (err) {
                        console.log("Error: ");
                        console.log(err);
                    }

                    if (res.rows.length) {
                        console.log("Added into TAF_forecast table. id: " + (res.rows[0])["id"]);
                    }

                    var taf_temp_forecast_id = (res.rows[0])["id"];

                    if ("clouds" in tempoArr[p] &&  (tempoArr[p]["clouds"] != null)) {
                        var tempCloudsArr = tempoArr[p]["clouds"];
                        for (var q in tempCloudsArr) {
                            var tempCloudsColumnSubStr = tafCloudsColumnSubStrTemplate;
                            var tempCloudsValuesSubStr = valuesSubStrTemplate;

                            tempCloudsColumnSubStr += "\"TAF_forecast_id\"",
                            tempCloudsValuesSubStr += taf_temp_forecast_id;

                            if ("abbreviation" in tempCloudsArr[q]) {
                                var tempCode = tempCloudsArr[q]["abbreviation"];
                                tempCloudsColumnSubStr += ", code";
                                tempCloudsValuesSubStr += (", '" + tempCode + "'");
                            }

                            if ("meaning" in tempCloudsArr[q]) {
                                var tempText = tempCloudsArr[q]["meaning"];
                                tempCloudsColumnSubStr += ", text";
                                tempCloudsValuesSubStr += (", '" + tempText + "'");
                            }

                            if ("altitude" in tempCloudsArr[q]) {
                                var tempBaseFeetAgl = tempCloudsArr[q]["altitude"];
                                tempCloudsColumnSubStr += ", base_feet_agl";
                                tempCloudsValuesSubStr += (", " + tempBaseFeetAgl);
                            }

                            var fullTempCloudsQuery = tempCloudsColumnSubStr + tempCloudsValuesSubStr + queryEndTemplate;

                            client.query(fullTempCloudsQuery, (err, res) => {
                                if (res.rows.length > 0) {
                                    console.log("Inserted record into TAF_forecast_clouds table. id: " + (res.rows[0])["id"]);
                                }
                            });
                        }
                    }

                    if ("weather" in tempoArr[p] && (tempoArr[p]["weather"] != null)) {
                        var tempConditionsArr = tempoArr[p]["weather"];
                        for (var r in tempConditionsArr) {
                            var tempConditionsColumnSubStr = tafConditionsColumnSubStrTemplate;
                            var tempConditionsValuesSubStr = valuesSubStrTemplate;
                            
                            tempConditionsColumnSubStr += "\"TAF_forecast_id\"";
                            tempConditionsValuesSubStr += taf_temp_forecast_id;

                            if ("abbreviation" in tempConditionsArr[r] && (tempConditionsArr[r] != null)) {
                                var tempCode = tempConditionsArr[r]["abbreviation"];
                                tempConditionsColumnSubStr += ", code";
                                tempConditionsValuesSubStr += (", '" + tempCode + "'");
                            }

                            if ("meaning" in tempConditionsArr[r] && (tempConditionsArr[r] != null)) {
                                var tempText = tempConditionsArr[r]["meaning"];
                                tempConditionsColumnSubStr += ", text";
                                tempConditionsValuesSubStr += (", '" + tempText + "'");
                            }

                            var fullTempConditionsQuery = tempConditionsColumnSubStr + tempConditionsValuesSubStr + queryEndTemplate;

                            client.query(fullTempConditionsQuery, (err, res) => {
                                if (res.rows.length > 0) {
                                    console.log("Insert record into TAF_forecast_conditions table. id:" + (res.rows[0])["id"])
                                }
                            });
                        }
                    }
                });
            }
        });
    });
}

fs.readFile("apikey.ini", function (err, data) {
    if (err) {
        throw err;
        process.exit();
    }
    apiKey = data;
    getTAFData();
    setInterval(getTAFData, 300000); // 300000 milliseconds = 5 minutes
});

