<head><style>html {
  color: #1a1a1a;
  background-color: #fdfdfd;
}
body {
  margin: 0 auto;
  max-width: 1500px;
  padding-left: 50px;
  padding-right: 50px;
  padding-top: 50px;
  padding-bottom: 50px;
  hyphens: auto;
  overflow-wrap: break-word;
  text-rendering: optimizeLegibility;
  font-kerning: normal;
  font-family: Georgia, serif;
}
img { max-width: 100%; }
table {
  width: 100%;
  border-collapse: collapse;
  margin: 1em auto;
  display: table;
}
th, td {
  padding: 0.5em;
  text-align: center;
  border: 1px solid #ddd;
}
figcaption, caption {
  font-style: italic;
  text-align: center;
  margin-top: 0.5em;
}
.tab-link {
  color: #2c5aa0;
  text-decoration: none;
  font-weight: 500;
  cursor: pointer;
}
.tab-link:hover {
  color: #1f3f73;
  text-decoration: underline;
}</style></head><h1 id="the-biochar-field-data-collection-process">The biochar field data collection process</h1>
<p>Data on soil volumetric water content (VWC), bulk electrical conductivity (EC), and temperature (°C) are collected from 12 Campbell Scientific CR206X data loggers, arranged 3 each in each of the 4 strips. Each data logger collects data from 3 Campbell Scientific CS650 Water Content Reflectometers, located at 6, 12, and 18 inches below the surface of the field.</p>
<p>The data flow from sensor to software has three steps – hard wired soil sensor to data logger, RF signal to a base datalogger, and finally from the datalogger to the final download site through a Starlink system, using an IPv6 56 bit prefix assigned by Starlink to the base datalogger, an ASUSRT-AX88U Pro wifi router. The router then assigns a 64 bit IPv6 address to the base data station, a CR680 data logger configured as a router.</p>
<h2 id="collecting-the-data">Collecting the data</h2>
<p>The CS650 is configured as a water content reflectometer, with the two parallel rods forming an open-ended transmission line. A differential oscillator circuit is connected to the rods, with an oscillator state change triggered by the return of a reflected signal from one of the rods. The two-way travel time of the electromagnetic waves that are induced by the oscillator on the rod varies with changing dielectric permittivity. Water is the main contributor to the bulk dielectric permittivity of the soil or porous media, so the travel time of the reflected wave increases with increasing water content and decreases with decreasing water content, hence the name water content reflectometer. The average travel time of the reflected wave multiplied by a scaling factor of 128 is called the period average. Period average is reported in microseconds and is the raw output of a water content reflectometer.</p>
<p>Electrical conductivity is determined by exciting the rods with a known non-polarizing waveform and measuring the signal attenuation. Signal attenuation is reported as a dimensionless voltage ratio, which is the ratio of the excitation voltage to the measured voltage along the sensor rods when they are excited at a fixed 100 kHz frequency. Voltage ratio ranges from 1 in nonconductive media to about 17 in highly conductive media. Values greater than 17 are highly unstable and indicate that the soil conditions are outside of the specified operating range of the sensor.</p>
<p>Temperature is measured with a thermistor in contact with one of the rods.</p>
<p>Source: Product Manual: CS650 and CS655 Water Content Reflectometers, Revision: 11/2021, Campbell Scientific, Inc. Available at the Campbell Scientific website - <a href="https://www.campbellsci.com"><u>https://www.campbellsci.com</u></a>.</p>
<h2 id="storing-and-transmitting-the-data-to-the-base-station">Storing and transmitting the data to the base station</h2>
<p>Each data logger includes a built-in spread spectrum radio (910 to 918 MHz, US version) for transferring data over long distances. It is powered by a solar panel and a 12 volt rechargeable battery. Each data logger has 512 kb of memory. The data logger is programmed to collect and store data from the sensors every 15 mins along with hourly and daily averages. When memory is full, the oldest data is removed and new data added. The data logger also has an RS-232 port that can be used to download data and upload programs.</p>
<h2 id="the-base-station-receives-data-from-the-data-loggers-and-transmits-it-to-a-final-destination-via-the-internet">The base station receives data from the data loggers and transmits it to a final destination via the internet</h2>
<p>The base station for this experiment combines a CR800 data logger configured for pass through, a spread spectrum radio to receive signals from the data logger and a networking device to take a signal from the data logger and convert it to an Ethernet signal. It needs 12-volt power which can be provided by a solar panel/battery combination or an AC to DC adapter.</p>
<h2 id="from-the-base-station-to-a-remote-data-management-site">From the base station to a remote data management site</h2>
<p>The experiment site is at a location with no internet access. To address this, the project uses a Starlink terminal connected to the base station through an ASUS WiFi router. The Starlink terminal assigns both an IPv4 and IPv6 address to the ASUS WiFi router. The Starlink IPv4 address can't use port forwarding so it is necessary to configure the ASUS router to accept and assign IPv6 addresses. The network device in the base station receives this address and provides access to the field dataloggers via the RF data transfer process.</p>
<p>The final stop for the data is at a remote location with a PC running the PC400 software from Campbell Scientific. The download process begins by establishing a connection from the PC to a datalogger and then choosing the data download tab after the connection is established. The new data (determined by a time stamp in each record of data) is appended to previously created .dat file (which is basically a .csv file with introductory metadata rows).</p>
<h2 id="code-availability">Code availability</h2>
<p>All the code elements are available on a github website. The current version is at this <a href="https://github.com/GeraldCNelson/biocharWaterConserveWebsite/tree/etl-refactor">link</a>.</p>
<h2 id="interpreting-ratios-why-temperature-ratios-are-not-reported">Interpreting Ratios: Why Temperature Ratios Are Not Reported</h2>
<p>Many of the plots and summary tables in this dashboard show ratios for volumetric water content (VWC) and electrical conductivity (EC). These ratios compare one strip or logger location to another (for example, S1/S2) and are often useful for seeing treatment effects or spatial differences. In contrast, we do not compute or display ratios for temperature variables.</p>
<p>At first glance it might seem that temperature ratios could be made meaningful by expressing temperatures in Kelvin, so that absolute zero corresponds to 0K and all values are strictly positive. However, even in Kelvin, a ratio such as <span class="math inline">$\frac{T_{experiment}}{T_{reference}}$</span> does not have a clear physical interpretation for soil or air temperature in this experiment. The processes we care about (plant growth, soil evaporation, microbial activity, etc.) respond more to temperature differences and thresholds (for example, days above or below a critical temperature) than to multiplicative changes in the absolute temperature value.</p>
<p>Temperature time series in this field are also highly seasonal and can cross 0 °C multiple times. When sensors at two locations differ only by a few degrees, the ratio can bounce around dramatically even though the absolute difference is small and biologically unimportant. In extreme cases, dividing by a value near freezing can produce very large or unstable ratios, which is the practical 'divide‑by‑zero' problem noted in the <a class="tab-link" data-tab="summary-tab" href="#">Summary Statistics</a> tab.</p>
<p>For these reasons, the project uses temperature mainly in its original units (°F or °C, depending on the display setting) and summarizes it with statistics such as minimum, mean, maximum, and standard deviation, rather than as a ratio between strips. Users interested in relative temperature behavior are encouraged to compare plots or summary tables directly (for example, comparing mean temperatures across strips or depths) instead of relying on a temperature ratio metric.</p>
