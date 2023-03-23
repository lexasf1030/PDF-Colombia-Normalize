/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for Inventory Balance Library                  ||
||                                                              ||
||  File Name: LMRY_CO_RptLibroDiarioDetallado_SCHDL_2.0.js     ||
||                                                              ||
||  Version Date         Author          Remarks                ||
||  2.0     Oct 12 2022  Jeferson Mejia  Use Script 2.0         ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType  ScheduledScript
 * @NModuleScope public
 */

define(["N/record", "N/runtime", "N/file", "N/encode", "N/search", "N/format", "N/log",
"N/config", "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js", 'N/render', "N/xml"
],

function(recordModulo, runtime, file, encode, search, format, log, config, libreriaGeneral,render, xml) {
// Nombre del Reporte
var objContext = runtime.getCurrentScript();
var namereport = "Reporte de Libro Diario (Detallado)";
var LMRY_script = 'LMRY CO Reportes Libro Diario SCHDL (Detallado)';
var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);
var GLOBAL_LABELS = getGlobalLabels();

//Parametros
var paramsubsidi = '';
var paramperiodo = '';
var paramMultibook = null;
var paramEndPeriod;
var paramFormatoReporte = '';
//Features
//Valida si es OneWorld
var featuresubs = runtime.isFeatureInEffect({
  feature: 'SUBSIDIARIES'
});
var feamultibook = runtime.isFeatureInEffect({
  feature: 'MULTIBOOK'
});
var featureMultipCalendars = runtime.isFeatureInEffect({
  feature: 'MULTIPLECALENDARS'
});
var FeaturePeriodEnd = runtime.isFeatureInEffect({
  feature: "PERIODENDJOURNALENTRIES"
});
//Datos Subsidiaria
var calendarSubsi = null;
var taxCalendarSubsi = null;
var companyruc = '';
var companyname = '';

//Control de Reporte
var periodstartdate = '';
var periodenddate = '';
var xlsString = '';
var ArrLibroDiario = new Array();
var arrAccountingContext = {};
var arrAccountingContextCuentaEnlazada = {};
var strName = '';
var periodname = '';
var auxmess = '';
var auxanio = '';
var multibook_name = '';
// Control de reporte
var monthStartD;
var yearStartD;

//PDF Normalization
var todays = "";
var currentTime = "";


var RecordName = 'customrecord_lmry_co_rpt_generator_log';
var RecordTable = ['custrecord_lmry_co_rg_name',
  'custrecord_lmry_co_rg_postingperiod',
  'custrecord_lmry_co_rg_subsidiary',
  'custrecord_lmry_co_rg_url_file',
  'custrecord_lmry_co_rg_employee',
  'custrecord_lmry_co_rg_multibook'
];

function execute(scriptContext) {
  try {
    obtenerParametrosYFeatures();
    ObtenerDatosSubsidiaria()
    obtenerPeriodosEspeciales(paramperiodo);

    ObtieneLibroDiario();
    log.debug('[execute] Nro. lineas movimientos', ArrLibroDiario.length);
    /*
    Evaluar number Account es donde se hara el cambio de cuenta.
    1.-Llenar un arreglo con el numero link lleno para cuentas.
    2.-Formar un json con ese arreglo usando como key el number fijo
    3.-la data de ese json es id puc 6 y denominacion
    4.-En evaluarNumberAccount, solo las cuentas que tengan ese campo lleno se reemplazaran.
    */
    /*
    if (paramMultibook != '1') {
      var array_context = ObtieneAccountingContext();
    }*/
    ObtieneAccountingContext();//Obtengo el numero de cuenta
    ObtieneAccountingContextNumeroFijo();//Obtengo los valores del puc
    EvaluarNumberAccount();
    log.debug('[execute] Nro. lineas movimientos despues del accounting context', ArrLibroDiario.length);
    

    todays = parseDateTo(new Date(), "DATE");
    currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

    if(paramFormatoReporte=='1'){
      log.debug("Se esta generando un pdf","Generando...");
      obtenerPdf();
    }else if(paramFormatoReporte == '0'){
      log.debug("Se esta generando un Excel","Generando...");
      obtenerEXCEl();
    }

  } catch (err) {
    log.error('[execute] Error de Schedule', err);
    
    return true;
  }
}

function obtenerParametrosYFeatures() {
  // Parametros
  try {
    paramsubsidi = objContext.getParameter({
      name: 'custscript_lmry_co_libdiariodet_subsi'
    });
    paramperiodo = objContext.getParameter({
      name: 'custscript_lmry_co_libdiariodet_period'
    });
    paramidlog = objContext.getParameter({
      name: 'custscript_lmry_co_libdiariodet_idlog'
    });
    paramMultibook = objContext.getParameter({
      name: 'custscript_lmry_co_libdiariodet_multi'
    });
    paramEndPeriod = objContext.getParameter({
      name: 'custscript_lmry_co_libdiariodet_adjust'
    });
    paramFormatoReporte = objContext.getParameter({
      name: 'custscript_lmry_co_libdiariodet_repform'
    });

    log.debug('Parametros', paramsubsidi + ', ' + paramperiodo + ', ' + paramidlog + ', ' + paramMultibook + ', ' + paramEndPeriod + ', ' + paramFormatoReporte);

    //validacion de existencia
    if (feamultibook == true || feamultibook == 'T') {
      var columna = search.lookupFields({
        type: 'accountingbook',
        id: paramMultibook,
        columns: ['name']
      });
      multibook_name = columna.name;
    }

  } catch (err) {
    log.debug('[obtenerParametrosYFeatures] Error', err)
  }
}
function obtenerPdf() {
  log.debug('language', language);
  ArrLibroDiario = AgruparInternalID();
  ArrLibroDiario = AgruparCuentasFechaTipo();

  if (ArrLibroDiario.length != null && ArrLibroDiario.length != 0) {
  var pdfString = "<?xml version=\"1.0\"?><!DOCTYPE pdf PUBLIC \"-//big.faceless.org//report\" \"report-1.1.dtd\">";
      pdfString += "<pdf>";
      pdfString += "<head>";
      pdfString += "<meta name = \"title\" value = \""+GLOBAL_LABELS["tituloPdf"][language]+"\" />";
      pdfString += "<macrolist>"
      pdfString += "<macro id=\"myheader\">";
      pdfString += "<table width=\"100%\"><tr><td colspan=\"3\" align =\"center\">"+GLOBAL_LABELS["Alert1"][language] +"</td></tr>";
      pdfString += "<tr>";
      pdfString += "<td colspan=\"3\" align =\"center\" width=\"40%\"><b> " + GLOBAL_LABELS["Alert2"][language]+" " + xml.escape(companyname) + "</b></td>";
      pdfString += "</tr>";
      pdfString += "<tr>";
      pdfString += "<td colspan=\"3\" align =\"center\" width=\"60%\"><b>NIT :"+ companyruc + "</b></td>";
      pdfString += "</tr>";
      pdfString += "<tr>";
      pdfString += "<td colspan=\"3\" align =\"center\" width=\"40%\"><b>"+GLOBAL_LABELS["Alert3"][language]+  periodstartdate + GLOBAL_LABELS["alConnector"][language] + periodenddate + "</b></td>";
      pdfString += "</tr>";
      if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {
        pdfString += "<tr>";
        pdfString += "<td colspan=\"3\" align =\"center\" width=\"60%\"><b>"+ GLOBAL_LABELS["Alert4"][language] + multibook_name + "</b></td>";
        pdfString += "</tr>";
      }

      pdfString += "<tr>";
      pdfString += "<td colspan=\"3\" align =\"center\" width=\"60%\"><b>" + GLOBAL_LABELS["origin"][language] + "Netsuite" + "</b></td>";
      pdfString += "</tr>";
      pdfString += "<tr>";
      pdfString += "<td colspan=\"3\" align =\"center\" width=\"60%\"><b>" + GLOBAL_LABELS["date"][language] + todays + "</b></td>";
      pdfString += "</tr>";
      pdfString += "<tr>";
      pdfString += "<td colspan=\"3\" align =\"center\" width=\"60%\"><b>" + GLOBAL_LABELS["time"][language] + currentTime + "</b></td>";
      pdfString += "</tr>";

      pdfString += "<tr>";
      pdfString += "</tr>";
      pdfString += "</table>";

      
      pdfString += "</macro>";
      pdfString += "<macro id=\"myfooter\">";
      pdfString += "<p align=\"right\">";
      pdfString += GLOBAL_LABELS["page"][language] + " <pagenumber/> " + GLOBAL_LABELS["of"][language] + " <totalpages/>";
      pdfString += "</p>";
      pdfString += "</macro>";
      pdfString += "</macrolist>";
      pdfString += "</head>";
      pdfString += "<body font-size=\"8\" header=\"myheader\" header-height=\"45mm\" footer=\"myfooter\" footer-height=\"20mm\" size=\"A4-LANDSCAPE\">";
  

      //Creacion de reporte pdf
      var fechaActual = ArrLibroDiario[0][0];
      var primerDia = ArrLibroDiario[0][0];
      var sumDebito = 0.00;
      var sumCredito = 0.00;
      var sumTotDebito = 0.00;
      var sumTotCredito = 0.00;
      var flag = 0;
      var existePrimeraCabecera = false;
      
   pdfString += "<table width=\"100%\">";
   
    //Titulo de las columnas
    pdfString += "<thead>";
    pdfString += "<tr>";
    pdfString += "<td colspan=\"1\" width=\"10%\" style=\"font-size: 1em\" ><b>"+GLOBAL_LABELS["Alert5"][language]+"</b></td>";
    pdfString += "<td colspan=\"1\" width=\"40%\" style=\"font-size: 1em\" ><b>"+GLOBAL_LABELS["Alert6"][language]+"</b></td>";
    pdfString += "<td colspan=\"1\" width=\"30%\" style=\"font-size: 1em\" ><b>"+GLOBAL_LABELS["Alert7"][language]+"</b></td>";
    pdfString += "<td colspan=\"1\" width=\"10%\" style=\"font-size: 1em\" align =\"right\"><b>"+GLOBAL_LABELS["Alert8"][language]+"</b></td>";
    pdfString += "<td colspan=\"1\" width=\"10%\" style=\"font-size: 1em\" align =\"right\"><b>"+GLOBAL_LABELS["Alert9"][language]+"</b></td>";
    pdfString += "</tr>";
    pdfString += "</thead>";

   for (var i = 0; i <= ArrLibroDiario.length - 1; i++) {

     var result = 0;
     result = Math.abs(Number(ArrLibroDiario[i][4])) - Math.abs(Number(ArrLibroDiario[i][5]));
     var primeraCabecera = ArrLibroDiario[i][0] === primerDia && flag === 0 && result !== 0;

     if (primeraCabecera) {
         pdfString += "<tr>";
         pdfString += "<td></td>"
         pdfString += "<td></td>"
         pdfString += "<td colspan=\"1\" width=\"40%\"><b>" + GLOBAL_LABELS["Alert10"][language] + fechaActual + "</b></td>";
         pdfString += "</tr>";
         flag++;
         existePrimeraCabecera = true;
     }

     if (fechaActual != ArrLibroDiario[i][0] && result != 0) {
         //arma el total de los quiebres
         if (existePrimeraCabecera || fechaActual != primerDia) {
             pdfString += "<tr>";
             pdfString += "<td></td>"
             pdfString += "<td></td>"
             pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle;\" ><b>" + GLOBAL_LABELS["Alert11"][language] + fechaActual + "</b></td>";
             pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle; text-align: right;\" align =\"right\">" + parseFloat(sumDebito).toFixed(2) + "</td>";
             pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle; text-align: right;\" align =\"right\">" + parseFloat(sumCredito).toFixed(2) + "</td>";
             pdfString += "</tr>";
             existePrimeraCabecera = false;
         }
         //Separador de montos totales
         pdfString += "<tr>";
         pdfString += "<td colspan=\"5\" width=\"40%\" style=\"border-bottom: 0px; vertical-align: middle;\" ></td>"
         pdfString += "</tr>";
         //Termina separador
         pdfString += "<tr>";
         pdfString += "<td></td>"
         pdfString += "<td></td>"
         fechaActual = ArrLibroDiario[i][0];
         sumCredito = 0.0;
         sumDebito = 0.0;

         //Nuevo quiebre
         pdfString += "<td colspan=\"1\" width=\"40%\"><b>" + GLOBAL_LABELS["Alert10"][language] + fechaActual + "</b></td>";
         pdfString += "</tr>";
     }

     if ((Number(ArrLibroDiario[i][4]) != 0 || Number(ArrLibroDiario[i][5]) != 0)) {

         if (result != 0) {

             if (result > 0) {
                 ArrLibroDiario[i][4] = result;
                 ArrLibroDiario[i][5] = 0;
             } else {
                 ArrLibroDiario[i][5] = result;
                 ArrLibroDiario[i][4] = 0;
             }
             pdfString += "<tr>";

             //1. Numero de Cuenta
             if ((ArrLibroDiario[i][1] != '' || ArrLibroDiario[i][1] != null)) {
                 pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle;\">" + ArrLibroDiario[i][1] + "</td>";
             } else {
                 pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle;\">" + "</td>";
             }

             //2. Denominaci?n
             if (ArrLibroDiario[i][2].length > 0) {
                 pdfString += "<td colspan=\"1\" width=\"40%\" style=\"border-bottom: 0px; vertical-align: middle;\" >" + ArrLibroDiario[i][2] + "</td>";
             } else {
                 pdfString += "<td colspan=\"1\" width=\"40%\" style=\"border-bottom: 0px; vertical-align: middle;\" >" + "</td>";
             }

             //3. Documento
             if (ArrLibroDiario[i][3] != '' || ArrLibroDiario[i][3] != null) {
                 pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle;\" >" + ArrLibroDiario[i][3] + "</td>";
             } else {
                 pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle;\" >" + "</td>";
             }

             //4. Suma Debito
             pdfString += "<td colspan=\"1\"  width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle; text-align: right;\" align =\"right\" >" + Math.abs(Number(ArrLibroDiario[i][4])).toFixed(2) + "</td>";

             //5. Suma Credito
             pdfString += "<td colspan=\"1\"  width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle; text-align: right; \" align =\"right\" >" + Math.abs(Number(ArrLibroDiario[i][5])).toFixed(2) + "</td>";

             sumDebito += Math.abs(Number(ArrLibroDiario[i][4]));
             sumTotDebito += Math.abs(Number(ArrLibroDiario[i][4]));

             sumCredito += Math.abs(Number(ArrLibroDiario[i][5]));
             sumTotCredito += Math.abs(Number(ArrLibroDiario[i][5]));

             pdfString += "</tr>";
         }
     }
   }

    //arma el total del ultimo quiebre
    pdfString += "<tr>";
    pdfString += "<td></td>"
    pdfString += "<td></td>"
    pdfString += "<td colspan=\"1\" width=\"10%\" ><b>"+ GLOBAL_LABELS["Alert11"][language]+ fechaActual + "</b></td>";
    pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle; text-align: right;\" align =\"right\">"+ parseFloat(sumDebito).toFixed(2)  +"</td>";
    pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle; text-align: right;\" align =\"right\">"+ parseFloat(sumCredito).toFixed(2) +"</td>";
    pdfString += "</tr>";

    //Separador de montos totales
    pdfString += "<tr>";
    pdfString += "<td colspan=\"5\" width=\"40%\" style=\"border-bottom: 0px; vertical-align: middle;\"></td>"
    pdfString += "</tr>";
    //Termina separador

    //arma el total del periodo
    pdfString += "<tr>";
    pdfString += "<td></td>"
    pdfString += "<td></td>"
    pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle;\" ><b>"+ GLOBAL_LABELS["Alert12"][language] + periodstartdate + ' al ' + periodenddate + "</b></td>";
    pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle; text-align: right;\" align =\"right\">"+ parseFloat(sumTotDebito).toFixed(2) +"</td>";
    pdfString += "<td colspan=\"1\" width=\"10%\" style=\"border-bottom: 0px; vertical-align: middle; text-align: right;\" align =\"right\">"+ parseFloat(sumTotCredito).toFixed(2) +"</td>";
    pdfString += "</tr>";

    // Cierra la tabla
    pdfString += '</table>';

    // Cierra cuerpo
    pdfString += '</body>';

    // Cierra el pdf
    pdfString += '</pdf>';

    Periodo(periodname);
    
    strName = pdfString;

    log.debug('strName',strName);
    
    if (paramMultibook != '' && paramMultibook != null) {
      var NameFile = "COLibroDiario_" + companyname + "_" + monthStartD + "_" + yearStartD + "_" + paramMultibook + ".pdf";
    } else {
      var NameFile = "COLibroDiario_" + companyname + "_" + monthStartD + "_" + yearStartD + ".pdf";
    }
    log.debug('obtenerPdf antes de savefile');
    savefile(NameFile, 'PDF');

  } else {
    var usuarioTemp = runtime.getCurrentUser(); //1.0 -> var usuario = objContext.getName();
    var usuario = usuarioTemp.name;

    if (paramidlog != null && paramidlog != '') {
      var record = recordModulo.load({
        type: RecordName,
        id: paramidlog
      });
      record.setValue(RecordTable[0], GLOBAL_LABELS["Alert13"][language]); // name
      /* record.setValue(RecordTable[1], periodname); */ // postingperiod
      record.setValue(RecordTable[2], companyname); // subsidiary
      /* record.setValue(RecordTable[4], usuario);  */ // employee
      if (feamultibook == true || feamultibook == 'T') {
        record.setValue(RecordTable[5], multibook_name); // multi
      }
      record.save();
      //libreria.sendrptuser(NameFile);
      //libreriaGeneral.sendConfirmUserEmail(namereport, 3, NameFile, language);
    }
  }

}
function obtenerEXCEl() {
  ArrLibroDiario = AgruparInternalID();
  ArrLibroDiario = AgruparCuentasFechaTipo();
  if (ArrLibroDiario.length != null && ArrLibroDiario.length != 0) {
    //cabecera de excel
    xlsString = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
    xlsString += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
    xlsString += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
    xlsString += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
    xlsString += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
    xlsString += 'xmlns:html="http://www.w3.org/TR/REC-html40">';

    // Propiedades del Documento
    xlsString += '<DocumentProperties xmlns="urn:schemas-microsoft-com:office:office">';
    xlsString += '<Author>' + companyname + '</Author>';
    xlsString += '<LastAuthor>' + companyname + '</LastAuthor>';
    xlsString += '<Created></Created>';
    xlsString += '<Company>' + companyname + '</Company>';
    xlsString += '<Version>2016.1.1</Version>';
    xlsString += '</DocumentProperties>';

    // Estilos de celdas
    xlsString += '<Styles>';
    xlsString += '<Style ss:ID="s20"><Font ss:Bold="1"/><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
    xlsString += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
    xlsString += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom" ss:WrapText="1"/></Style>';
    xlsString += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Horizontal="Right" ss:Vertical="Bottom"/></Style>';
    xlsString += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* #,##0.00_);_(* \(#,##0.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
    xlsString += '</Styles>';

    // Nombre de la hoja
    xlsString += '<Worksheet ss:Name="Libro Diario">';

    xlsString += '<Table>';
    xlsString += '<Column ss:AutoFitWidth="0" ss:Width="060"/>';
    xlsString += '<Column ss:AutoFitWidth="0" ss:Width="220"/>';
    xlsString += '<Column ss:AutoFitWidth="0" ss:Width="140"/>';
    xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
    xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
    //Cabecera
    xlsString += '<Row>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert1"][language] + '</Data></Cell>';
    xlsString += '</Row>';
    xlsString += '<Row></Row>';
    xlsString += '<Row>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"> ' + GLOBAL_LABELS["Alert2"][language] + companyname + '</Data></Cell>';
    xlsString += '</Row>';
    xlsString += '<Row>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">NIT: ' + companyruc + '</Data></Cell>';
    xlsString += '</Row>';
    xlsString += '<Row>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["Alert3"][language] + periodstartdate + ' al ' + periodenddate + '</Data></Cell>';
    xlsString += '</Row>';
    if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["Alert4"][language] + multibook_name + '</Data></Cell>';
      xlsString += '</Row>';
    }
    xlsString += '<Row>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["origin"][language] + "Netsuite" + '</Data></Cell>';
    xlsString += '</Row>';
    xlsString += '<Row>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["date"][language] + todays + '</Data></Cell>';
    xlsString += '</Row>';
    xlsString += '<Row>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["time"][language] + currentTime + '</Data></Cell>';
    xlsString += '</Row>';
    // Una linea en blanco
    xlsString += '<Row></Row>';
    // Titulo de las columnas
    xlsString += '<Row></Row>';
    xlsString += '<Row>' +
      '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert5"][language] + '</Data></Cell>' +
      '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert6"][language] + '</Data></Cell>' +
      '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert7"][language] + '</Data></Cell>' +
      '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert8"][language] + '</Data></Cell>' +
      '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert9"][language] + ' </Data></Cell>' +
      '</Row>';


//Creacion de reporte xls
var fechaActual = ArrLibroDiario[0][0];
var primerDia = ArrLibroDiario[0][0];
var sumDebito = 0.00;
var sumCredito = 0.00;
var sumTotDebito = 0.00;
var sumTotCredito = 0.00;
var flag = 0; // Esto es para que solo se imprima 1 vez en el primer dia
var existePrimeraCabecera = false; //! MEJORAR TODA ESTA LOGICA EN UNA PROXIMA VERSION, mejorar el control de cambios de fecha

for (var i = 0; i <= ArrLibroDiario.length - 1; i++) {

   var result = 0;
   result = Math.abs(Number(ArrLibroDiario[i][4])) - Math.abs(Number(ArrLibroDiario[i][5]));
   var primeraCabecera = ArrLibroDiario[i][0] === primerDia && flag === 0 && result !== 0;

   if (primeraCabecera) {
       xlsString += '<Row>';
       xlsString += '<Cell></Cell>';
       xlsString += '<Cell></Cell>';
       xlsString += '<Cell ss:StyleID="s22" ss:MergeAcross="1"><Data ss:Type="String">' + GLOBAL_LABELS["Alert10"][language] + fechaActual + '</Data></Cell>';
       xlsString += '</Row>';
       flag++;
       existePrimeraCabecera = true;
   }


   if (fechaActual != ArrLibroDiario[i][0] && result != 0) {
       //arma el total de los quiebres
       if (existePrimeraCabecera || fechaActual != primerDia) {
           xlsString += '<Row>';
           xlsString += '<Cell></Cell>';
           xlsString += '<Cell ss:StyleID="s23" ss:MergeAcross="1"><Data ss:Type="String">' + GLOBAL_LABELS["Alert11"][language] + fechaActual + '</Data></Cell>';
           xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(sumDebito).toFixed(2) + '</Data></Cell>';
           xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(sumCredito).toFixed(2) + '</Data></Cell>';
           xlsString += '</Row>';
           existePrimeraCabecera = false;
       }

       fechaActual = ArrLibroDiario[i][0];
       log.debug('cambio fecha -> ', fechaActual);

       sumCredito = 0.0;
       sumDebito = 0.0;
       //Nuevo quiebre
       // MergeAcross = "1" (Junta dos columnas) "2" (Junta tres columnas), junta una mas del parametro que se pasa
       xlsString += '<Row>';
       xlsString += '<Cell></Cell>';
       xlsString += '<Cell></Cell>';
       xlsString += '<Cell ss:StyleID="s22" ss:MergeAcross="1"><Data ss:Type="String">' + GLOBAL_LABELS["Alert10"][language] + fechaActual + '</Data></Cell>';
       xlsString += '</Row>';
   }


   if ((Number(ArrLibroDiario[i][4]) != 0 || Number(ArrLibroDiario[i][5]) != 0)) {

       if (result != 0) {

           if (result > 0) {
               ArrLibroDiario[i][4] = result;
               ArrLibroDiario[i][5] = 0;
           } else {
               ArrLibroDiario[i][5] = result;
               ArrLibroDiario[i][4] = 0;
           }

           xlsString += '<Row>';

           //1. Numero de Cuenta
           if ((ArrLibroDiario[i][1] != '' || ArrLibroDiario[i][1] != null)) {
               xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + ArrLibroDiario[i][1] + '</Data></Cell>';
           } else {
               xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
           }

           //2. Denominaci?n
           if (ArrLibroDiario[i][2].length > 0) {
               xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + ArrLibroDiario[i][2] + '</Data></Cell>';
           } else {
               xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
           }

           //3. Documento
           if (ArrLibroDiario[i][3] != '' || ArrLibroDiario[i][3] != null) {
               xlsString += '<Cell><Data ss:Type="String">' + ArrLibroDiario[i][3] + '</Data></Cell>';
           } else {
               xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
           }

           //4. Suma Debito

           xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(ArrLibroDiario[i][4])) + '</Data></Cell>';

           //5. Suma Credito
           xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(ArrLibroDiario[i][5])) + '</Data></Cell>';

           sumDebito += Math.abs(Number(ArrLibroDiario[i][4]));
           sumTotDebito += Math.abs(Number(ArrLibroDiario[i][4]));

           sumCredito += Math.abs(Number(ArrLibroDiario[i][5]));
           sumTotCredito += Math.abs(Number(ArrLibroDiario[i][5]));

           xlsString += '</Row>';
       }
   }
 } 

    //arma el total del ultimo quiebre
    xlsString += '<Row>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell ss:StyleID="s23" ss:MergeAcross="1"><Data ss:Type="String">' + GLOBAL_LABELS["Alert11"][language] + fechaActual + '</Data></Cell>';
    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(sumDebito).toFixed(2) + '</Data></Cell>';
    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(sumCredito).toFixed(2) + '</Data></Cell>';
    xlsString += '</Row>';

    //arma el total del periodo
    xlsString += '<Row>';
    xlsString += '<Cell></Cell>';
    xlsString += '<Cell ss:MergeAcross="1" ss:StyleID="s23"><Data ss:Type="String">' + GLOBAL_LABELS["Alert12"][language] + periodstartdate + ' al ' + periodenddate + '</Data></Cell>';
    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(sumTotDebito).toFixed(2) + '</Data></Cell>';
    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(sumTotCredito).toFixed(2) + '</Data></Cell>';
    xlsString += '</Row>';

    // Cierra la tabla
    xlsString += '</Table>';

    // Cierra la Hoja de Trabajo 1
    xlsString += '</Worksheet>';

    // Cierra el Libro
    xlsString += '</Workbook>';

    Periodo(periodname);
    //Se arma el archivo EXCEL
    strName = encode.convert({
      string: xlsString,
      inputEncoding: encode.Encoding.UTF_8,
      outputEncoding: encode.Encoding.BASE_64
    });

    if (paramMultibook != '' && paramMultibook != null) {
      var NameFile = "COLibroDiario_" + companyname + "_" + monthStartD + "_" + yearStartD + "_" + paramMultibook + ".xls";
    } else {
      var NameFile = "COLibroDiario_" + companyname + "_" + monthStartD + "_" + yearStartD + ".xls";
    }
    savefile(NameFile, 'EXCEL');
  } else {
    var usuarioTemp = runtime.getCurrentUser(); //1.0 -> var usuario = objContext.getName();
    var usuario = usuarioTemp.name;

    if (paramidlog != null && paramidlog != '') {
      var record = recordModulo.load({
        type: RecordName,
        id: paramidlog
      });
      record.setValue(RecordTable[0], GLOBAL_LABELS["Alert13"][language]); // name
      /* record.setValue(RecordTable[1], periodname); */ // postingperiod
      record.setValue(RecordTable[2], companyname); // subsidiary
      /* record.setValue(RecordTable[4], usuario);  */ // employee
      if (feamultibook == true || feamultibook == 'T') {
        record.setValue(RecordTable[5], multibook_name); // multi
      }
      record.save();
      //libreria.sendrptuser(NameFile);
      //libreriaGeneral.sendConfirmUserEmail(namereport, 3, NameFile, language);

    }
  }
}

function EvaluarNumberAccount() {
  for (var i = 0; i < ArrLibroDiario.length; i++) {
    if (paramMultibook != 1) {
      if (ArrLibroDiario[i][6] == 'Bank' || ArrLibroDiario[i][6] == 'Accounts Payable' || ArrLibroDiario[i][6] == 'Accounts Receivable' ||
        ArrLibroDiario[i][6] == 'Banco' || ArrLibroDiario[i][6] == 'Cuentas a pagar' || ArrLibroDiario[i][6] == 'Cuentas a cobrar') {
        
        var cuenta_act = obtenerCuenta(ArrLibroDiario[i][9]);
        var den_act = obtenerCuentaDenominacion(ArrLibroDiario[i][9]);
        
        if (cuenta_act == ''/*ArrLibroDiario[i][7]*/) {
          ArrLibroDiario[i][1] = ArrLibroDiario[i][1];
        } else {
          ArrLibroDiario[i][1] = cuenta_act;
        }

        if (den_act == ''/*ArrLibroDiario[i][7]*/) {
          ArrLibroDiario[i][2] = ArrLibroDiario[i][2];
        } else {
          ArrLibroDiario[i][2] = den_act;
        }
      } else {
        ArrLibroDiario[i][1] = ArrLibroDiario[i][1];
        ArrLibroDiario[i][2] = ArrLibroDiario[i][2];
      }
    } else {
      ArrLibroDiario[i][1] = ArrLibroDiario[i][1];
      ArrLibroDiario[i][2] = ArrLibroDiario[i][2];
    }
  }

}

function AgruparInternalID() {
  var ArrReturn = [];

  for (var i = 0; i < ArrLibroDiario.length; i++) {
    var sumDebit = 0;
    var sumCredit = 0;

    for (var j = i; j < ArrLibroDiario.length; j++) {
      if (ArrLibroDiario[i][8] == ArrLibroDiario[j][8] && ArrLibroDiario[i][1] == ArrLibroDiario[j][1] && ArrLibroDiario[i][7] == ArrLibroDiario[j][7]) {
        sumDebit += Math.abs(Number(ArrLibroDiario[j][4]));
        sumCredit += Math.abs(Number(ArrLibroDiario[j][5]));
      } else {
        var ArrTemp = [];
        ArrTemp.push(ArrLibroDiario[i][0]);
        ArrTemp.push(ArrLibroDiario[i][1]);
        ArrTemp.push(ArrLibroDiario[i][2]);
        ArrTemp.push(ArrLibroDiario[i][3]);
        ArrTemp.push(sumDebit);
        ArrTemp.push(sumCredit);
        ArrTemp.push(ArrLibroDiario[i][6]);
        ArrTemp.push(ArrLibroDiario[i][7]);
        ArrTemp.push(ArrLibroDiario[i][8]);

        ArrReturn.push(ArrTemp);

        i = j - 1;
        break;
      }

      if (i == ArrLibroDiario.length - 1) {
        var ArrTemp = [];
        ArrTemp.push(ArrLibroDiario[i][0]);
        ArrTemp.push(ArrLibroDiario[i][1]);
        ArrTemp.push(ArrLibroDiario[i][2]);
        ArrTemp.push(ArrLibroDiario[i][3]);
        ArrTemp.push(sumDebit);
        ArrTemp.push(sumCredit);
        ArrTemp.push(ArrLibroDiario[i][6]);
        ArrTemp.push(ArrLibroDiario[i][7]);
        ArrTemp.push(ArrLibroDiario[i][8]);

        ArrReturn.push(ArrTemp);

      }
    }
  }

  return ArrReturn;
}

function AgruparCuentasFechaTipo() {
  var semilla = new Array();
  var vectFin = [];
  var i = 0;
  semilla = ArrLibroDiario[0];

  var montoDebit = 0;
  var montoCredit = 0;
  while (i < ArrLibroDiario.length) {

    if (ArrLibroDiario[i][0] == semilla[0] && ArrLibroDiario[i][1] == semilla[1] && ArrLibroDiario[i][3] == semilla[3]) {

      montoDebit += Number(ArrLibroDiario[i][4]);
      montoCredit += Number(ArrLibroDiario[i][5]);
      ArrLibroDiario.splice(i, 1);

      if (i == ArrLibroDiario.length) {
        //Se agrega al vector final de vector de Cuentas Segun las mismas condiciones
        vectFin.push(AsignadDatosVect(semilla, montoDebit, montoCredit));
        if (ArrLibroDiario.length > 0) {
          montoDebit = 0;
          montoCredit = 0;
          semilla = ArrLibroDiario[0];
          i = 0;
        } else {
          break;
        }
      }
    } else {
      if (i == ArrLibroDiario.length - 1) {
       //  if (semilla[1] != ArrLibroDiario[i][1] && semilla[3] != ArrLibroDiario[i][3] && semilla[8] != ArrLibroDiario[i][8]) {
         vectFin.push(AsignadDatosVect(semilla, montoDebit, montoCredit));
        montoDebit = 0;
        montoCredit = 0;
        semilla = ArrLibroDiario[0];
        i = 0;
       //  }
      } else {
        i++;
      }
    }
  }
  return vectFin;
}

function AsignadDatosVect(semilla, montoDebit, montoCredit) {

  var vectorF = [];
  for (var i = 0; i < semilla.length; i++) {
    if (i != 4 && i != 5) {
      vectorF[i] = semilla[i];
    } else {
      if (i == 4) {
        vectorF[i] = montoDebit;
      } else {
        vectorF[i] = montoCredit;
      }
    }
  }
  return vectorF;
}

function ObtieneLibroDiario() {

  // Seteo de Porcentaje completo
  objContext.percentComplete = 0.00;
  // Control de Memoria
  var intDMaxReg = 1000;
  var intDMinReg = 0;
  var arrAuxiliar = new Array();

  // Exedio las unidades
  var DbolStop = false;
  var _cont = 0;

  // Consulta de Cuentas
  if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {
    /* Latamready - CO Diary Book */
    var savedsearch = search.load({
      type: 'accountingtransaction',
      id: 'customsearch_lmry_co_libro_diario'
    });

    savedsearch.filters.push(
      search.createFilter({
        name: 'postingperiod',
        join: 'transaction',
        operator: 'is',
        values: [paramperiodo]
      })
    );
    savedsearch.filters.push(
      search.createFilter({
        name: 'accountingbook',
        operator: 'is',
        values: [paramMultibook]
      })
    );

    savedsearch.columns.push(search.createColumn({
      name: 'internalid',
      summary: 'GROUP'
    }));

    // Valida si es OneWorld
    if (featuresubs) {
      savedsearch.filters.push(
        search.createFilter({
          name: 'subsidiary',
          operator: 'is',
          values: [paramsubsidi]
        })
      );
    }
    //9.-Tran Id or transaction number
    var tranIdNumberColumn = search.createColumn({
      name: 'formulatext',
      formula: 'NVL({transaction.tranid},{transaction.transactionnumber})',
      summary: 'GROUP',
      label: '9.-Tran Id or transaction number'
    });
    savedsearch.columns.push(tranIdNumberColumn);

    
  } else {

    var savedsearch = search.load({
      type: search.Type.TRANSACTION,
      id: 'customsearch_lmry_co_libro_diario_trans'
    });

    savedsearch.filters.push(
      search.createFilter({
        name: 'postingperiod',
        operator: 'is',
        values: [paramperiodo]
      })
    );

    savedsearch.columns.push(search.createColumn({
      name: 'internalid',
      summary: 'GROUP'
    }));
    // Valida si es OneWorld
    if (featuresubs) {
      savedsearch.filters.push(
        search.createFilter({
          name: 'subsidiary',
          operator: 'is',
          values: [paramsubsidi]
        })
      );
    }
    //9.-Tran Id or transaction number
    var tranIdNumberColumn = search.createColumn({
      name: 'formulatext',
      formula: 'NVL({tranid},{transactionnumber})',
      summary: 'GROUP',
      label: '9.-Tran Id or transaction number'
    });
    savedsearch.columns.push(tranIdNumberColumn);
  }

  
  //10.- account number
  var accountInternalIdColumn = search.createColumn({
    name: 'formulanumeric',
    formula: '{account.internalid}',
    summary: 'GROUP',
    label: '10.-Account internal id'
  });
  savedsearch.columns.push(accountInternalIdColumn);

  var searchresult = savedsearch.run();

  while (!DbolStop) {
    var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
    if (objResult != null) {
      var intLength = objResult.length;

      for (var i = 0; i < intLength; i++) {
        columns = objResult[i].columns;
        arrAuxiliar = new Array();
        var tranIdCamp = objResult[i].getValue(columns[9]);

        //0. fecha
        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
          arrAuxiliar[0] = objResult[i].getValue(columns[0]);
        else
          arrAuxiliar[0] = '';
        //1. cuenta
        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
          arrAuxiliar[1] = objResult[i].getValue(columns[1]);
        else
          arrAuxiliar[1] = '';
        //2. denominacion
        if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
          arrAuxiliar[2] = objResult[i].getValue(columns[2]);
        else
          arrAuxiliar[2] = '';
        //3. documento
        if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined'){
          if (tranIdCamp != null && tranIdCamp != '- None -' && tranIdCamp != 'NaN' && tranIdCamp != 'undefined'){
            arrAuxiliar[3] = objResult[i].getText(columns[3]) +' - ' + tranIdCamp;
          }else{
            arrAuxiliar[3] = objResult[i].getText(columns[3]);
          }
        }else
          arrAuxiliar[3] = '';
        //4. sum debitos

        if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
          arrAuxiliar[4] = objResult[i].getValue(columns[4]);
        else
          arrAuxiliar[4] = 0.00;
        //5. sum credito
        if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
          arrAuxiliar[5] = objResult[i].getValue(columns[5])
        //.toFixed(2);
        else
          arrAuxiliar[5] = 0.00;

        //6. tipo de cuenta
        if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
          arrAuxiliar[6] = objResult[i].getText(columns[6]);
        else
          arrAuxiliar[6] = '';

        //7. Numero de cuenta
        if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
          arrAuxiliar[7] = objResult[i].getValue(columns[7]);
        else
          arrAuxiliar[7] = '';

        //8. Internal ID Tx
        if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
          arrAuxiliar[8] = objResult[i].getValue(columns[8]);
        else
          arrAuxiliar[8] = '';
        
          //9. Account Internal ID
        if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 'NaN' && objResult[i].getValue(columns[10]) != 'undefined')
          arrAuxiliar[9] = objResult[i].getValue(columns[10]);
        else
          arrAuxiliar[9] = '';

        ArrLibroDiario[_cont] = arrAuxiliar;
        _cont++;

      }
      intDMinReg = intDMaxReg;
      intDMaxReg += 1000;
      if (intLength < 1000) {
        DbolStop = true;
      }
    } else {
      DbolStop = true;
    }
  }
  log.debug('[ObtieneLibroDiario] Movimientos', ArrLibroDiario.length);
  //busqueda sobre los period end journal
  var myDate = format.parse({
    value: periodenddate,
    type: format.Type.DATE
  });

  mes_generado = myDate.getMonth() + 1;
  anio_generado = myDate.getFullYear();

  if ((feamultibook || feamultibook == 'T')) {
    var savedsearch2 = search.load({
      type: 'accountingtransaction',
      id: 'customsearch_lmry_co_libro_diario_jour'
    });

    savedsearch2.filters.push(
      search.createFilter({
        name: 'postingperiod',
        join: 'transaction',
        operator: 'is',
        values: [paramperiodo]
      })
    );
    savedsearch2.filters.push(
      search.createFilter({
        name: 'accountingbook',
        operator: 'is',
        values: [paramMultibook]
      })
    );
    savedsearch2.columns.push(search.createColumn({
      name: 'internalid',
      operator: 'null',
      summary: search.Summary.GROUP
    }));

    // Valida si es OneWorld
    if (featuresubs) {
      savedsearch2.filters.push(
        search.createFilter({
          name: 'subsidiary',
          operator: 'is',
          values: [paramsubsidi]
        })
      );
    }

    //10.-Tran Id or transaction number
    var tranIdNumberColumn = search.createColumn({
      name: 'formulatext',
      formula: 'NVL({transaction.number},{transaction.transactionnumber})',
      summary: 'GROUP',
      label: '9.-Tran Id or transaction number'
    });
    savedsearch2.columns.push(tranIdNumberColumn);

    var intDMaxReg = 1000;
    var intDMinReg = 0;
    var arrAuxiliar = new Array();
    // Exedio las unidades
    var DbolStop = false;

   } else {
     var savedsearch2 = search.create({
       type: 'transaction',
       filters:
       [
           ["posting","is","T"], 
           "AND", 
           ["formulatext: {account.custrecord_lmry_co_puc_d6_description}","isnotempty",""], 
           "AND", 
           ["formulanumeric: CASE {account.custrecord_lmry_localbook} WHEN 'T' THEN 1 ELSE 0 END","equalto","1"],
           "AND", 
           ["type","anyof","PEJrnl"]
       ],
       columns:
       [
           search.createColumn({
             name: "trandate",
             summary: "GROUP",
             sort: search.Sort.ASC,
             label: "FECHA"
           }),
           search.createColumn({
             name: "formulatext",
             summary: "GROUP",
             formula: "{account.custrecord_lmry_co_puc_d6_id}",
             sort: search.Sort.ASC,
             label: "CUENTA"
           }),
           search.createColumn({
             name: "formulatext",
             summary: "GROUP",
             formula: "{account.custrecord_lmry_co_puc_d6_description}",
             sort: search.Sort.ASC,
             label: "DENOMINACION"
           }),
           search.createColumn({
             name: "type",
             summary: "GROUP",
             label: "DOCUMENTO"
           }),
           search.createColumn({
             name: "formulacurrency",
             summary: "SUM",
             formula: "NVL({debitamount},0)",
             label: "Formula (Currency)"
           }),
           search.createColumn({
             name: "formulacurrency",
             summary: "SUM",
             formula: "NVL({creditamount},0)",
             label: "Formula (Currency)"
           }),
           search.createColumn({
             name: "type",
             summary: "GROUP",
             label: "Type"
           }),
           search.createColumn({
             name: "number",
             join: "account",
             summary: "GROUP",
             label: "Number"
           })
       ]
     });

     savedsearch2.filters.push(
       search.createFilter({
         name: 'postingperiod',
         operator: 'is',
         values: [paramperiodo]
       })
     );
     savedsearch2.columns.push(search.createColumn({
       name: 'internalid',
       summary: search.Summary.GROUP
     }));

     // Valida si es OneWorld
     if (featuresubs) {
       savedsearch2.filters.push(
         search.createFilter({
           name: 'subsidiary',
           operator: 'is',
           values: [paramsubsidi]
         })
       );
     }

     //10.-Tran Id or transaction number
     var tranIdNumberColumn = search.createColumn({
       name: 'formulatext',
       formula: 'NVL({transaction.number},{transaction.transactionnumber})',
       summary: 'GROUP',
       label: '9.-Tran Id or transaction number'
     });
     savedsearch2.columns.push(tranIdNumberColumn);

     var intDMaxReg = 1000;
     var intDMinReg = 0;
     var arrAuxiliar = new Array();
     // Exedio las unidades
     var DbolStop = false;
   }
    //11.- account internal id
    var accountInternalIdColumn = search.createColumn({
      name: 'formulanumeric',
      formula: '{account.internalid}',
      summary: 'GROUP',
      label: '11.-Account internal id'
    });
    savedsearch2.columns.push(accountInternalIdColumn);

   


    var searchresult = savedsearch2.run();
    while (!DbolStop) {
      var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

      if (objResult != null) {
        var intLength = objResult.length;

        for (var i = 0; i < intLength; i++) {
          columns = objResult[i].columns;
          arrAuxiliar = new Array();
          
          var tranIdCamp = objResult[i].getValue(columns[9]);
          
          //0. fecha
          if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
          else
            arrAuxiliar[0] = '';
          //1. cuenta
          if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
          else
            arrAuxiliar[1] = '';
          //2. denominacion
          if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
          else
            arrAuxiliar[2] = '';
          //3. documento
          if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined'){
            if (tranIdCamp != null && tranIdCamp != '- None -' && tranIdCamp != 'NaN' && tranIdCamp != 'undefined'){
              arrAuxiliar[3] = objResult[i].getText(columns[3]) +' - ' + tranIdCamp;
            }else{
              arrAuxiliar[3] = objResult[i].getText(columns[3]);
            }
          }
          else
            arrAuxiliar[3] = '';
          //4. sum debitos

          if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
            arrAuxiliar[4] = objResult[i].getValue(columns[4]);
          else
            arrAuxiliar[4] = 0.00;
          //5. sum credito
          if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
            arrAuxiliar[5] = objResult[i].getValue(columns[5])
          //.toFixed(2);
          else
            arrAuxiliar[5] = 0.00;

          //6. tipo de cuenta
          if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
            arrAuxiliar[6] = objResult[i].getText(columns[6]);
          else
            arrAuxiliar[6] = '';

          //7. Numero de cuenta
          if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
            arrAuxiliar[7] = objResult[i].getValue(columns[7]);
          else
            arrAuxiliar[7] = '';

          //8. Internal ID Tx
          if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
            arrAuxiliar[8] = objResult[i].getValue(columns[8]);
          else
            arrAuxiliar[8] = '';

          //9. Account Internal ID
          if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 'NaN' && objResult[i].getValue(columns[10]) != 'undefined')
            arrAuxiliar[9] = objResult[i].getValue(columns[10]);
          else
            arrAuxiliar[9] = '';

          ArrLibroDiario[_cont] = arrAuxiliar;
          _cont++;

        }
        intDMinReg = intDMaxReg;
        intDMaxReg += 1000;
        if (intLength < 1000) {
          DbolStop = true;
        }
      } else {
        DbolStop = true;
      }
    

      log.debug('[ObtieneLibroDiario] MovimientosJournal', ArrLibroDiario.length);

   }
  //se vuelven a ejecutar las busquedas para en caso el check de ajuste esta activo

  var accountingperiodSearch = search.create({
    type: search.Type.ACCOUNTING_PERIOD,
    filters: [
      ["isadjust", "is", "T"],
      "AND", ["periodname", "contains", String(anio_generado)],
    ],
    columns: [
      search.createColumn({
        name: "periodname",
        sort: search.Sort.ASC,
      }),
      search.createColumn({
        name: "internalid"
      })
    ]
  });
  /////////////////////////ojo-bug////////////////////////////////////
  var IDAdjust = '';
  var resultadosAccounting = accountingperiodSearch.run().getRange(0, 1000);
  if (accountingperiodSearch.length != 0) {
    for (var n = 0; n < resultadosAccounting.length; n++) {
      var row = resultadosAccounting[n];
      var columns = row.columns;
      IDAdjust = row.getValue(columns[1]);
    }
  }

  if (paramEndPeriod == 'T' && IDAdjust != '' && mes_generado == 12) {
    intDMaxReg = 1000;
    intDMinReg = 0;
    arrAuxiliar = new Array();

    // Exedio las unidades
    DbolStop = false;
    if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {

      var savedsearch = search.load({
        type: 'accountingtransaction',
        id: 'customsearch_lmry_co_libro_diario'
      });

      savedsearch.filters.push(
        search.createFilter({
          name: 'postingperiod',
          join: 'transaction',
          operator: 'is',
          values: [IDAdjust]
        })
      );
      savedsearch.filters.push(
        search.createFilter({
          name: 'accountingbook',
          operator: 'is',
          values: [paramMultibook]
        })
      );
      savedsearch.columns.push(search.createColumn({
        name: 'internalid',
        summary: 'GROUP'
      }));

      // Valida si es OneWorld
      if (featuresubs) {
        savedsearch.filters.push(
          search.createFilter({
            name: 'subsidiary',
            operator: 'is',
            values: [paramsubsidi]
          })
        );
      }
      //9.-Tran Id or transaction number
      var tranIdNumberColumn = search.createColumn({
        name: 'formulatext',
        formula: 'NVL({transaction.tranid},{transaction.transactionnumber})',
        summary: 'GROUP',
        label: '9.-Tran Id or transaction number'
      });
      savedsearch.columns.push(tranIdNumberColumn);
      
    } else {

      var savedsearch = search.load({
        type: 'transaction',
        id: 'customsearch_lmry_co_libro_diario_trans'
      });

      savedsearch.filters.push(
        search.createFilter({
          name: 'postingperiod',
          operator: 'is',
          values: [IDAdjust]
        })
      );

      savedsearch.columns.push(search.createColumn({
        name: 'internalid',
        summary: 'GROUP'
      }));
      // Valida si es OneWorld
      if (featuresubs) {
        savedsearch.filters.push(
          search.createFilter({
            name: 'subsidiary',
            operator: 'is',
            values: [paramsubsidi]
          })
        );
      }
      //9.-Tran Id or transaction number
      var tranIdNumberColumn = search.createColumn({
        name: 'formulatext',
        formula: 'NVL({tranid},{transactionnumber})',
        summary: 'GROUP',
        label: '9.-Tran Id or transaction number'
      });
      savedsearch.columns.push(tranIdNumberColumn);
    }
    //10.-account internal id
    var accountInternalIdColumn = search.createColumn({
      name: 'formulanumeric',
      formula: '{account.internalid}',
      summary: 'GROUP',
      label: '10.-Account internal id'
    });
    savedsearch.columns.push(accountInternalIdColumn);

    var searchresult = savedsearch.run();

    while (!DbolStop) {
      var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

      if (objResult != null) {
        var intLength = objResult.length;

        for (var i = 0; i < intLength; i++) {
          columns = objResult[i].columns;
          arrAuxiliar = new Array();

          var tranIdCamp = objResult[i].getValue(columns[9]);
        
          //0. fecha
          if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
          else
            arrAuxiliar[0] = '';
          //1. cuenta
          if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
          else
            arrAuxiliar[1] = '';
          //2. denominacion
          if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
          else
            arrAuxiliar[2] = '';
          //3. documento
          if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined'){
            if (tranIdCamp != null && tranIdCamp != '- None -' && tranIdCamp != 'NaN' && tranIdCamp != 'undefined'){
              arrAuxiliar[3] = objResult[i].getText(columns[3]) +' - ' + tranIdCamp;
            }else{
              arrAuxiliar[3] = objResult[i].getText(columns[3]);
            }
          }else
            arrAuxiliar[3] = '';
          //4. sum debitos

          if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
            arrAuxiliar[4] = objResult[i].getValue(columns[4]);
          else
            arrAuxiliar[4] = 0.00;
          //5. sum credito
          if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
            arrAuxiliar[5] = objResult[i].getValue(columns[5])
          //.toFixed(2);
          else
            arrAuxiliar[5] = 0.00;

          //6. tipo de cuenta
          if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
            arrAuxiliar[6] = objResult[i].getText(columns[6]);
          else
            arrAuxiliar[6] = '';

          //7. Numero de cuenta
          if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
            arrAuxiliar[7] = objResult[i].getValue(columns[7]);
          else
            arrAuxiliar[7] = '';

          //8. Internal ID Tx
          if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
            arrAuxiliar[8] = objResult[i].getValue(columns[8]);
          else
            arrAuxiliar[8] = '';

          //9. Account Internal ID
          if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 'NaN' && objResult[i].getValue(columns[10]) != 'undefined')
            arrAuxiliar[9] = objResult[i].getValue(columns[10]);
          else
            arrAuxiliar[9] = '';
          

          ArrLibroDiario[_cont] = arrAuxiliar;
          _cont++;

        }
        intDMinReg = intDMaxReg;
        intDMaxReg += 1000;
        if (intLength < 1000) {
          DbolStop = true;
        }
      } else {
        DbolStop = true;
      }
    }

    log.debug('[ObtieneLibroDiario] Movimientos2', ArrLibroDiario.length);

    //busqueda de period end jorunal con el id de ajuste
    if ((feamultibook || feamultibook == 'T') && FeaturePeriodEnd) {
      intDMaxReg = 1000;
      intDMinReg = 0;
      arrAuxiliar = new Array();

      // Exedio las unidades
      DbolStop = false;

      var savedsearch2 = search.load({
        type: 'accountingtransaction',
        id: 'customsearch_lmry_co_libro_diario_jour'
      });
      savedsearch2.filters.push(
        search.createFilter({
          name: 'postingperiod',
          join: 'transaction',
          operator: 'is',
          values: [IDAdjust]
        })
      );
      savedsearch2.filters.push(
        search.createFilter({
          name: 'accountingbook',
          operator: 'is',
          values: [paramMultibook]
        })
      );
      savedsearch2.columns.push(search.createColumn({
        name: 'internalid',
        summary: search.Summary.GROUP
      }));

      // Valida si es OneWorld
      if (featuresubs) {
        savedsearch2.filters.push(
          search.createFilter({
            name: 'subsidiary',
            operator: 'is',
            values: [paramsubsidi]
          })
        );
      }
      //10.-Tran Id or transaction number
      var tranIdNumberColumn = search.createColumn({
        name: 'formulatext',
        formula: 'NVL({transaction.number},{transaction.transactionnumber})',
        summary: 'GROUP',
        label: '10.-Tran Id or transaction number'
      });
      savedsearch.columns.push(tranIdNumberColumn);
      
      var intDMaxReg = 1000;
      var intDMinReg = 0;
      var arrAuxiliar = new Array();
      //11.-account internal id
      var accountInternalIdColumn = search.createColumn({
        name: 'formulanumeric',
        formula: '{account.internalid}',
        summary: 'GROUP',
        label: '11.-Account internal id'
      });
      savedsearch.columns.push(accountInternalIdColumn);

      // Exedio las unidades
      var DbolStop = false;
      var searchresult = savedsearch2.run();
      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            columns = objResult[i].columns;
            arrAuxiliar = new Array();

            var tranIdCamp = objResult[i].getValue(columns[10]);
          
            //0. fecha
            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            else
              arrAuxiliar[0] = '';
            //1. cuenta
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            else
              arrAuxiliar[1] = '';
            //2. denominacion
            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
              arrAuxiliar[2] = objResult[i].getValue(columns[2]);
            else
              arrAuxiliar[2] = '';
            //3. documento
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined'){
              if (tranIdCamp != null && tranIdCamp != '- None -' && tranIdCamp != 'NaN' && tranIdCamp != 'undefined'){
                arrAuxiliar[3] = objResult[i].getText(columns[3]) +' - ' + tranIdCamp;
              }else{
                arrAuxiliar[3] = objResult[i].getText(columns[3]);
              }
            }else
              arrAuxiliar[3] = '';
            //4. sum debitos

            if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
              arrAuxiliar[4] = objResult[i].getValue(columns[4]);
            else
              arrAuxiliar[4] = 0.00;
            //5. sum credito
            if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
              arrAuxiliar[5] = objResult[i].getValue(columns[5])
            //.toFixed(2);
            else
              arrAuxiliar[5] = 0.00;

            //6. tipo de cuenta
            if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
              arrAuxiliar[6] = objResult[i].getText(columns[6]);
            else
              arrAuxiliar[6] = '';

            //7. Numero de cuenta
            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
              arrAuxiliar[7] = objResult[i].getValue(columns[7]);
            else
              arrAuxiliar[7] = '';

            //8. Internal ID Tx
            if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
              arrAuxiliar[8] = objResult[i].getValue(columns[8]);
            else
              arrAuxiliar[8] = '';

            //9. Account Internal ID
            if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'NaN' && objResult[i].getValue(columns[11]) != 'undefined')
              arrAuxiliar[9] = objResult[i].getValue(columns[11]);
            else
              arrAuxiliar[9] = '';

            ArrLibroDiario[_cont] = arrAuxiliar;
            _cont++;

          }
          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
          if (intLength < 1000) {
            DbolStop = true;
          }
        } else {
          DbolStop = true;
        }
      }
      
    }
  }
  log.debug('[ObtieneLibroDiario] MovimientosJournal2', ArrLibroDiario.length);
}

function savefile(pNombreFile, pTipoArchivo) {
  // Ruta de la carpeta contenedora
  var FolderId = objContext.getParameter({
    name: 'custscript_lmry_file_cabinet_rg_co'
  });
  // Almacena en la carpeta de Archivos Generados
  if (FolderId != '' && FolderId != null) {
    // Genera el nombre del archivo
    var NameFile = pNombreFile;
    if(pTipoArchivo=='EXCEL'){
      // Crea el archivo
      var Filed = file.create({
        name: NameFile,
        fileType: pTipoArchivo,
        contents: strName,
        folder: FolderId
      });

      var idfile = Filed.save();
    }else{
      //Crea el archivo PDF
      var transaccionesFile = render.xmlToPdf({
        xmlString : strName,
      });
      transaccionesFile.name = NameFile;
      transaccionesFile.folder = FolderId;
      var idfile = transaccionesFile.save();
    }

    
    var idfile2 = file.load({
      id: idfile
    });
    // Obtenemo de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
    var getURL = objContext.getParameter({
      name: 'custscript_lmry_netsuite_location'
    });
    var urlfile = '';
    if (getURL != '' && getURL != '') {
      urlfile += 'https://' + getURL;
    }
    urlfile += idfile2.url;

    //Genera registro personalizado como log
    if (idfile) {
      var usuarioTemp = runtime.getCurrentUser(); // var usuario = objContext.getName();
      var usuario = usuarioTemp.name;
      // Se graba el en log de archivos generados del reporteador
      var record = recordModulo.load({
        type: RecordName,
        id: paramidlog
      });

      record.setValue({
        fieldId: RecordTable[0],
        value: NameFile
      }); //name
      record.setValue({
        fieldId: RecordTable[2],
        value: companyname
      }); //subsidiary
      record.setValue({
        fieldId: RecordTable[3],
        value: urlfile
      }); //url_file
      if (feamultibook || feamultibook == 'T')
        record.setValue({
          fieldId: RecordTable[5],
          value: multibook_name
        });

      record.save();
      // Envia mail de conformidad al usuario
      //libreria.sendrptuser(namereport, 3, NameFile);
      //libreria.sendrptuser(NameFile);
      libreriaGeneral.sendConfirmUserEmail(namereport, 3, NameFile, language);
    }
  } else {
    // Debug
    log.error('Creacion de Excel', 'No se existe el folder');
  }
}

function ObtenerDatosSubsidiaria() {
  var configpage = config.load({
    type: config.Type.COMPANY_INFORMATION
  });

  if (featuresubs == true || featuresubs == 'T') { //EN ALGUNAS INSTANCIAS DEVUELVE CADENA OTRAS DEVUELVE BOOLEAN
    if (featureMultipCalendars || featureMultipCalendars == 'T') {
      var subsidyName = search.lookupFields({
        type: search.Type.SUBSIDIARY,
        id: paramsubsidi,
        columns: ['legalname', 'taxidnum', 'fiscalcalendar', 'taxfiscalcalendar']
      });
      //NO SE VALIDA EL CAMPO FISCAL/TAX CALENDAR PORQUE ES OBLIGATORIO EN LA SUBSIDIARIA
      calendarSubsi = {
        id: subsidyName.fiscalcalendar[0].value,
        nombre: subsidyName.fiscalcalendar[0].text
      }
      calendarSubsi = JSON.stringify(calendarSubsi);

      taxCalendarSubsi = {
        id: subsidyName.taxfiscalcalendar[0].value,
        nombre: subsidyName.taxfiscalcalendar[0].text
      }
      taxCalendarSubsi = JSON.stringify(taxCalendarSubsi);
    } else {
      var subsidyName = search.lookupFields({
        type: search.Type.SUBSIDIARY,
        id: paramsubsidi,
        columns: ['legalname', 'taxidnum']
      });

    }
    companyname = subsidyName.legalname;
    companyruc = subsidyName.taxidnum;

  } else {
    companyName = configpage.getValue('companyname');
    companyruc = configpage.getValue('employerid');
  }


}

function obtenerPeriodosEspeciales(paramperiod) {
  //valida si el accounting special ...
  var licenses = libreriaGeneral.getLicenses(paramsubsidi);
  featAccountingSpecial = libreriaGeneral.getAuthorization(677, licenses); //true o false, 677

  if (featAccountingSpecial || featAccountingSpecial == 'T') {
    var searchSpecialPeriod = search.create({
      type: "customrecord_lmry_special_accountperiod",
      filters: [
        ["isinactive", "is", "F"],
        'AND', ["custrecord_lmry_accounting_period", "is", paramperiod]
      ],
      columns: [
        search.createColumn({
          name: "custrecord_lmry_date_ini",
          label: "1. Latam - Date Start",
        }),
        search.createColumn({
          name: "custrecord_lmry_date_fin",
          label: "2. Latam - Date Fin",
        }),
        search.createColumn({
          name: "name",
          label: "3. Latam - Period Name",
        })
      ]

    });
    if (featureMultipCalendars || featureMultipCalendars == 'T') {
      var fiscalCalendarFilter = search.createFilter({
        name: 'custrecord_lmry_calendar',
        operator: search.Operator.IS,
        values: calendarSubsi
      });
      searchSpecialPeriod.filters.push(fiscalCalendarFilter);
    }

    var pagedData = searchSpecialPeriod.runPaged({
      pageSize: 1000
    });

    pagedData.pageRanges.forEach(function(pageRange) {
      page = pagedData.fetch({
        index: pageRange.index
      });

      page.data.forEach(function(result) {
        columns = result.columns;
        periodstartdate = result.getValue(columns[0]);
        periodenddate = result.getValue(columns[1]);
        periodname = result.getValue(columns[2]);

      })
    });
  } else {
    if (paramperiodo != null && paramperiodo != '') {
      var columnFrom = search.lookupFields({
        type: 'accountingperiod',
        id: paramperiodo,
        columns: ['enddate', 'periodname', 'startdate']
      });

      periodstartdate = columnFrom.startdate;
      periodenddate = columnFrom.enddate;
      periodname = columnFrom.periodname;

    }
  }

  var tempdate = format.parse({
    value: periodstartdate,
    type: format.Type.DATE
  });
  monthStartD = tempdate.getMonth() + 1;

  if (('' + monthStartD).length == 1) {
    monthStartD = '0' + monthStartD;
  } else {
    monthStartD = monthStartD + '';
  }
  yearStartD = tempdate.getFullYear();
}

function Periodo(periodo) {
  var auxfech = '';

  auxanio = periodo.substr(-4);
  switch (periodo.substring(0, 3).toLowerCase()) {
    case 'jan':
      auxmess = '01';
      break;
    case 'ene':
      auxmess = '01';
      break;
    case 'feb':
      auxmess = '02';
      break;
    case 'mar':
      auxmess = '03';
      break;
    case 'abr':
      auxmess = '04';
      break;
    case 'apr':
      auxmess = '04';
      break;
    case 'may':
      auxmess = '05';
      break;
    case 'jun':
      auxmess = '06';
      break;
    case 'jul':
      auxmess = '07';
      break;
    case 'ago':
      auxmess = '08';
      break;
    case 'aug':
      auxmess = '08';
      break;
    case 'set':
      auxmess = '09';
      break;
    case 'sep':
      auxmess = '09';
      break;
    case 'oct':
      auxmess = '10';
      break;
    case 'nov':
      auxmess = '11';
      break;
    case 'dec':
      auxmess = '12';
      break;
    case 'dic':
      auxmess = '12';
      break;
    default:
      auxmess = '00';
      break;
  }
  auxfech = auxanio + auxmess + '00';
  return;
}

function ObtieneAccountingContext() {
  // Control de Memoria
  var intDMaxReg = 1000;
  var intDMinReg = 0;
  var arrAuxiliar = new Array();
  var contador_auxiliar = 0;
  var DbolStop = false;

  var savedsearch = search.load({
    type: 'account',
    id: 'customsearch_lmry_account_context'
  });
  // Valida si es OneWorld
  if (featuresubs == true) {
    savedsearch.filters.push(
      search.createFilter({
        name: 'subsidiary',
        operator: 'is',
        values: [paramsubsidi]
      })
    );
  }
  var col_search_puc6_id = search.createColumn({
    name: 'formulatext',
    summary: 'group',
    formula: '{custrecord_lmry_co_puc_d6_id}'
  });

  savedsearch.columns.push(col_search_puc6_id);

  var col_search_puc6_den = search.createColumn({
    name: 'formulatext',
    summary: 'group',
    formula: '{custrecord_lmry_co_puc_d6_description}'
  });

  savedsearch.columns.push(col_search_puc6_den);

  var col_account_internalid = search.createColumn({
    name: 'formulanumeric',
    summary: 'group',
    formula: '{internalid}'
  });
  savedsearch.columns.push(col_account_internalid);

  var searchresult = savedsearch.run();

  while (!DbolStop) {
    var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

    if (objResult != null) {
      var intLength = objResult.length;
      log.debug('intDMinReg, intDMaxReg, length: ', intDMinReg + ',' + intDMaxReg + ',' + intLength);

      if (intLength == 0) {
        DbolStop = true;
      } else {
        for (var i = 0; i < intLength; i++) {
          // Cantidad de columnas de la busqueda
          columns = objResult[i].columns;
          arrAuxiliar = new Array();
          for (var col = 0; col < columns.length; col++) {
             if (col == 3) {
                 if (objResult[i].getText(columns[col]) != null && objResult[i].getText(columns[col]) != '- None -' && objResult[i].getText(columns[col]) != 'NaN' && objResult[i].getText(columns[col]) != 'undefined'){
                     arrAuxiliar[col] = objResult[i].getText(columns[col]);
                 }else{
                     arrAuxiliar[col] = '';
                 }                     
             } else {
               if (objResult[i].getText(columns[col]) != null && objResult[i].getText(columns[col]) != '- None -' && objResult[i].getText(columns[col]) != 'NaN' && objResult[i].getText(columns[col]) != 'undefined'){
                 arrAuxiliar[col] = objResult[i].getText(columns[col]);
               }else{
                 arrAuxiliar[col] = '';
               }
             }
          }
          
          if (arrAuxiliar[3] == multibook_name) {
            var key_json= arrAuxiliar[3] + '|' + arrAuxiliar[6]; 
            arrAccountingContext[key_json] = arrAuxiliar;
          }
        }
      }

      intDMinReg = intDMaxReg;
      intDMaxReg += 1000;
    } else {
      DbolStop = true;
    }
  }
}
function ObtieneAccountingContextNumeroFijo() {
  // Control de Memoria
  var intDMaxReg = 1000;
  var intDMinReg = 0;
  var arrAuxiliar = new Array();
  var contador_auxiliar = 0;
  var DbolStop = false;

  var savedsearch = search.create({
    type: search.Type.ACCOUNT,
    columns: []
  });
  
  savedsearch.filters.push(
    search.createFilter({
      name: 'formulatext',
      formula: '{custrecord_lmry_desp_cta_cont_corporativ}',
      operator: 'isnotempty'
    })
  );
    /*
  // Valida si es OneWorld
  if (featuresubs == true) {
    savedsearch.filters.push(
      search.createFilter({
        name: 'subsidiary',
        operator: 'is',
        values: [paramsubsidi]
      })
    );
  }
  */
  var col_search_puc6_id = search.createColumn({
    name: 'formulatext',
    summary: 'group',
    formula: '{custrecord_lmry_co_puc_d6_id}'
  });

  savedsearch.columns.push(col_search_puc6_id);

  var col_search_puc6_den = search.createColumn({
    name: 'formulatext',
    summary: 'group',
    formula: '{custrecord_lmry_co_puc_d6_description}'
  });

  savedsearch.columns.push(col_search_puc6_den);

  var col_search_cta_cont_corporativ = search.createColumn({
    name: 'formulatext',
    summary: 'group',
    formula: '{custrecord_lmry_desp_cta_cont_corporativ}'
  });

  savedsearch.columns.push(col_search_cta_cont_corporativ);


  var searchresult = savedsearch.run();

  while (!DbolStop) {
    var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

    if (objResult != null) {
      var intLength = objResult.length;

      if (intLength == 0) {
        DbolStop = true;
      } else {
        for (var i = 0; i < intLength; i++) {
          // Cantidad de columnas de la busqueda
          columns = objResult[i].columns;
          arrAuxiliar = new Array();
          if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined'){
           arrAuxiliar[0] = objResult[i].getValue(columns[0]); 
          }else{
           arrAuxiliar[0] = '';
          }
          if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined'){
           arrAuxiliar[1] = objResult[i].getValue(columns[1]);
          }else{
           arrAuxiliar[1] = '';
          }
          if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined'){
           arrAuxiliar[2] = objResult[i].getValue(columns[2]);
          }else{
           arrAuxiliar[2] = '';
          }
          arrAccountingContextCuentaEnlazada[arrAuxiliar[2]] = arrAuxiliar;
        }
      }
      intDMinReg = intDMaxReg;
      intDMaxReg += 1000;
    } else {
      DbolStop = true;
    }
  }
}
function obtenerCuenta(internalid_cuenta){
  /*
  for (var i = 0; i < arrAccountingContext.length; i++) {

    if (numero_cuenta == arrAccountingContext[i][0]) {

      var number_cta_aux = arrAccountingContext[i][1];
      for (var j = 0; j < arrAccountingContext.length; j++) {

        if (number_cta_aux == arrAccountingContext[j][0]) {
          return arrAccountingContext[j][4];
        }
      }
    }
  }
  return numero_cuenta;
  */
 var key_json = multibook_name+ '|' +internalid_cuenta; 
 if(arrAccountingContext[key_json]!=undefined){
  var localNumber = arrAccountingContext[key_json][1];
  if(arrAccountingContextCuentaEnlazada[localNumber]!=undefined){
   var cuentaId = arrAccountingContextCuentaEnlazada[localNumber][0];
  }else{
    return '';
  }
  return cuentaId;
}else{
  return '';
 }
}

function obtenerCuentaDenominacion(internalid_cuenta) {
  /*
  for (var i = 0; i < arrAccountingContext.length; i++) {
    if (numero_cuenta == arrAccountingContext[i][0]) {
      var number_cta_aux = arrAccountingContext[i][1];
      for (var j = 0; j < arrAccountingContext.length; j++) {

        if (number_cta_aux == arrAccountingContext[j][0]) {
          return arrAccountingContext[j][5];
        }
      }
    }
  }
  return numero_cuenta;*/
  var key_json = multibook_name+ '|' +internalid_cuenta; 

  if(arrAccountingContext[key_json]!=undefined){
    var localNumber = arrAccountingContext[key_json][1];
    if(arrAccountingContextCuentaEnlazada[localNumber]!=undefined){
      var cuentaDenominacion = arrAccountingContextCuentaEnlazada[localNumber][1];
    }else{
       return '';
    }

    return cuentaDenominacion;
  }else{
    return '';
   }

}

function getGlobalLabels() {
  var labels = {
    "Alert1": {
      "es": "LIBRO DIARIO",
      "en": "DIARY BOOK :",
      "pt": "LiVRO DIÁRIO :"
    },
    "Alert2": {
      "es": "Razon Social :",
      "en": "Business Name :",
      "pt": "Razão Social"
    },
    "Alert3": {
      "es": "Periodo :",
      "en": "Period :",
      "pt": "Período :"
    },
    "Alert4": {
      "es": "Multibooking :",
      "en": "Multibooking :",
      "pt": "Multibooking :"
    },
    "Alert5": {
      "es": "Cuenta",
      "en": "Account",
      "pt": "Conta"
    },
    "Alert6": {
      "es": "Denominación",
      "en": "Denomination",
      "pt": "Denominação"
    },
    "Alert7": {
      "es": "Documento",
      "en": "Document",
      "pt": "Documento"
    },
    "Alert8": {
      "es": "Debito",
      "en": "Debit",
      "pt": "Débito"
    },
    "Alert9": {
      "es": "Credito",
      "en": "Credit",
      "pt": "Crédito"
    },
    "Alert10": {
      "es": "Movimientos del dia ",
      "en": "Movements of the day ",
      "pt": "Movimentos do dia "
    },
    "Alert11": {
      "es": "Total movimientos del dia",
      "en": "Total movements of the day ",
      "pt": "Total de movimentos do dia "
    },
    "Alert12": {
      "es": "Total movimientos del periodo ",
      "en": "Total movements of the period ",
      "pt": "Movimentos totais do período "
    },
    "Alert13": {
      "es": "No existe informacion para los criterios seleccionados",
      "en": "There is no information for the selected criteria",
      "pt": "Não há informações para os critérios selecionados"
    },
    "tituloPdf":{
      "es": "LIBRO DIARIO",
      "en": "DIARY BOOK",
      "pt": "LIVRO DIÁRIO"
    },
    "alConnector":{
      "es": " al ",
      "en": " to ",
      "pt": " a "
    },
    "origin": {
      "es": "Origen :",
      "en": "Origin :",
      "pt": "Origem :"
    },
    "date": {
      "es": "Fecha :",
      "en": "Date :",
      "pt": "Data :"
    },
    "time": {
      "es": "Hora :",
      "en": "Time :",
      "pt": "Hora :"
    },
    "page": {
      "es": "Página",
      "en": "Page",
      "pt": "Página"
    },
    "of": {
      "es": "de",
      "en": "of",
      "pt": "de"
    }
  };

  return labels;
}

function parseDateTo(trandate, type) {
  var $date = '';

  if (!trandate) return;

  // In Scheduled or Map/Reduce scripts the user timezone is not available
  var userObj = runtime.getCurrentUser();
  var userPrefTime = userObj.getPreference({ name: 'TIMEZONE' });

  $date = format.format({ value: trandate, type: format.Type[type], timezone: userPrefTime });

  return $date;
}

//** Function used to Get Current Time by DAYTIME*/
function getTimeHardcoded(datetime){

  if (!datetime) return;

  // This is provider by NetSuite Settings > User Preferences > Time Format
  var timeFormat = {
      "h:mm a": ":",
      "H:mm": ":",
      "h-mm a": "-",
      "H-mm": "-",
  }

  var userObj = runtime.getCurrentUser();
  var userPrefTimeFormat = userObj.getPreference({ name: 'TIMEFORMAT' });

  var separator = timeFormat[userPrefTimeFormat];

  var time = datetime.split(" ")[1];
  var ampm = datetime.split(" ")[2];

  var hours = time.split(separator)[0];
  var minutes = time.split(separator)[1];

  var time_ampm = hours + separator + minutes + " " + ampm;
  time = hours + separator + minutes;

  return  (ampm) ? time_ampm : time;
}

return {
  execute: execute
};
});