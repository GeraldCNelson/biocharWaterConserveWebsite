export {};

declare var Plotly: any;

declare global {
  interface Window {
    Plotly?: any;
    bootstrap?: any;
    markdownit?: any;
    MathJax?: any;

    unitSystem?: string;
    depthMapping?: Record<string, any>;
    loggerLocationMapping?: Record<string, any>;
    dropdownOptions?: any;
    dateRanges?: Record<string, any>;
    variableNameMapping?: Record<string, any>;
    labelNameMapping?: Record<string, any>;
    gseasonPeriods?: Record<string, any>;
    CUSTOM_GSEASON_CONFIG?: any;

    __lastSummaryData?: any;
    latestSummaryStats?: any;
    __bulkDownloadManifest?: any;

    _plotRenderWidth?: number | null;
    _plotRightGutter?: number | null;
    _plotLeftMargin?: number | null;
    _plotLegendMode?: string | null;
    _initialXRange?: any;
    _biocharResizePlotsInstalled?: boolean;

    mainDatepickers?: any;

    downloadTraceData?: any;
    downloadPlot?: any;
    downloadSummaryData?: any;
  }
}