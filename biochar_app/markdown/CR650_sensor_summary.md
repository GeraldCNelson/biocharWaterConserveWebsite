<!-- Auto-generated from `CS650_sensor_summary.tex` -->
<!-- math via MathJax -->

<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" lang="" xml:lang="">
<head>
  <meta charset="utf-8" />
  <meta name="generator" content="pandoc" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes" />
  <title>CR650_sensor_summary</title>
  <style>
    html {
      color: #1a1a1a;
      background-color: #fdfdfd;
    }
    body {
      margin: 0 auto;
      max-width: 36em;
      padding-left: 50px;
      padding-right: 50px;
      padding-top: 50px;
      padding-bottom: 50px;
      hyphens: auto;
      overflow-wrap: break-word;
      text-rendering: optimizeLegibility;
      font-kerning: normal;
    }
    @media (max-width: 600px) {
      body {
        font-size: 0.9em;
        padding: 12px;
      }
      h1 {
        font-size: 1.8em;
      }
    }
    @media print {
      html {
        background-color: white;
      }
      body {
        background-color: transparent;
        color: black;
        font-size: 12pt;
      }
      p, h2, h3 {
        orphans: 3;
        widows: 3;
      }
      h2, h3, h4 {
        page-break-after: avoid;
      }
    }
    p {
      margin: 1em 0;
    }
    a {
      color: #1a1a1a;
    }
    a:visited {
      color: #1a1a1a;
    }
    img {
      max-width: 100%;
    }
    svg {
      height: auto;
      max-width: 100%;
    }
    h1, h2, h3, h4, h5, h6 {
      margin-top: 1.4em;
    }
    h5, h6 {
      font-size: 1em;
      font-style: italic;
    }
    h6 {
      font-weight: normal;
    }
    ol, ul {
      padding-left: 1.7em;
      margin-top: 1em;
    }
    li > ol, li > ul {
      margin-top: 0;
    }
    blockquote {
      margin: 1em 0 1em 1.7em;
      padding-left: 1em;
      border-left: 2px solid #e6e6e6;
      color: #606060;
    }
    code {
      font-family: Menlo, Monaco, Consolas, 'Lucida Console', monospace;
      font-size: 85%;
      margin: 0;
      hyphens: manual;
    }
    pre {
      margin: 1em 0;
      overflow: auto;
    }
    pre code {
      padding: 0;
      overflow: visible;
      overflow-wrap: normal;
    }
    .sourceCode {
     background-color: transparent;
     overflow: visible;
    }
    hr {
      border: none;
      border-top: 1px solid #1a1a1a;
      height: 1px;
      margin: 1em 0;
    }
    table {
      margin: 1em 0;
      border-collapse: collapse;
      width: 100%;
      overflow-x: auto;
      display: block;
      font-variant-numeric: lining-nums tabular-nums;
    }
    table caption {
      margin-bottom: 0.75em;
    }
    tbody {
      margin-top: 0.5em;
      border-top: 1px solid #1a1a1a;
      border-bottom: 1px solid #1a1a1a;
    }
    th {
      border-top: 1px solid #1a1a1a;
      padding: 0.25em 0.5em 0.25em 0.5em;
    }
    td {
      padding: 0.125em 0.5em 0.25em 0.5em;
    }
    header {
      margin-bottom: 4em;
      text-align: center;
    }
    #TOC li {
      list-style: none;
    }
    #TOC ul {
      padding-left: 1.3em;
    }
    #TOC > ul {
      padding-left: 0;
    }
    #TOC a:not(:hover) {
      text-decoration: none;
    }
    code{white-space: pre-wrap;}
    span.smallcaps{font-variant: small-caps;}
    div.columns{display: flex; gap: min(4vw, 1.5em);}
    div.column{flex: auto; overflow-x: auto;}
    div.hanging-indent{margin-left: 1.5em; text-indent: -1.5em;}
    /* The extra [class] is a hack that increases specificity enough to
       override a similar rule in reveal.js */
    ul.task-list[class]{list-style: none;}
    ul.task-list li input[type="checkbox"] {
      font-size: inherit;
      width: 0.8em;
      margin: 0 0.8em 0.2em -1.6em;
      vertical-align: middle;
    }
  </style>
  <script
  src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-chtml-full.js"
  type="text/javascript"></script>
</head>
<body>
<h1 class="unnumbered"
id="cs650-frequency-domain-reflectometer-fdr-sensor">CS650
Frequency-Domain Reflectometer (FDR) Sensor</h1>
<p>The CS650 Water Content Reflectometer measures volumetric water
content (VWC) by probing the soil’s dielectric constant via
high-frequency electromagnetic waves. Its key specifications and
operating principles are:</p>
<ul>
<li><p><strong>Measurement Principle:</strong> Frequency-Domain
Reflectometry (FDR) at a nominal frequency of 70 MHz. A burst of RF
energy is emitted along the waveguide rods; the reflected signal phase
shift is proportional to the soil’s dielectric permittivity <span
class="math inline">\(\varepsilon\)</span>.</p></li>
<li><p><strong>Probe Geometry &amp; Volume:</strong> Three
stainless-steel waveguides, 10 cm long and 2.5 mm diameter, form an
equilateral triangle. The effective sampling volume is approximately a
cylinder of length 10 cm and radius 3–5 cm around the probes.</p></li>
<li><p><strong>Calibration &amp; Conversion:</strong> The raw phase
shift <span class="math inline">\(\phi\)</span> is processed inside the
CR6 datalogger to compute <span
class="math inline">\(\varepsilon\)</span>, then converted to VWC (<span
class="math inline">\(\theta\)</span>) using a factory-tuned calibration
curve: <span class="math display">\[\theta = a_0 + a_1\,\varepsilon +
a_2\,\varepsilon^2\,,\]</span> where <span class="math inline">\(a_0,
a_1, a_2\)</span> are sensor-specific coefficients.</p></li>
<li><p><strong>Accuracy &amp; Resolution:</strong></p>
<ul>
<li><p>Typical accuracy: <span class="math inline">\(\pm0.02\)</span>
(±2</p></li>
<li><p>Resolution: 0.0001 m³/m³</p></li>
<li><p>Response time: ≪1 s</p></li>
</ul></li>
<li><p><strong>Environmental Range:</strong> <span
class="math inline">\(\theta\in[0,\,0.5]\)</span> m³/m³; soil
temperature range −40 °C to +60 °C.</p></li>
<li><p><strong>Data Logging:</strong> Paired with a Campbell Scientific
CR6 datalogger, which timestamps and stores measurements at user-defined
intervals, and can apply temperature and bulk density corrections in
real time.</p></li>
</ul>
</body>
</html>
