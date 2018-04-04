# AirportWxApiData
A server-side node.js application to store real-time airport weather data into a postgresql database.

## Background Information
This application retrieves real-time weather data from [api.checkwx.com](https://api.checkwx.com). This requires a registration for an API key (free) from CheckWX first.

# Built With
* [metar.js](https://github.com/theciezac/metar.js) - A fork of [skydivejkl's](https://github.com/skydivejkl) [metar.js](https://github.com/skydivejkl/metar.js), customised to parse and decode METAR reports from Singapore Changi Airport (ICAO: WSSS)
* [taf.js](https://github.com/theciezac/taf.js) - A separate version of [skydivejkl's](https://github.com/skydivejkl) [metar.js](https://github.com/skydivejkl/metar.js), customised to parse and decode TAF reports from Singapore Changi Airport (ICAO: WSSS)

## Authors
* **theciezac.net** - *Initial work* - [Github profile](https://github.com/theciezac)

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## Acknowledgements
* [CheckWX](https://www.checkwx.com)
* [skydivejkl](https://github.com/skydivejkl)
