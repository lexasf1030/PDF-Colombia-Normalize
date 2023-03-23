 /* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
    ||   This script for customer center (Time)                     ||
    ||                                                              ||
    ||  File Name: LMRY_CO_ReporteMagAnualF1008v7.1_MPRD_V2.0.js    ||
    ||                                                              ||
    ||  Version Date         Author        Remarks                  ||
    ||  2.0     May 24 2021  LatamReady    Use Script 2.0           ||
    \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
 /**
  * @NApiVersion 2.0
  * @NScriptType MapReduceScript
  * @NModuleScope Public
  */
 define(["N/record", "N/runtime", "N/file", "N/search", "N/format",
         "N/log", "N/config", "N/encode", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js"
     ],

     function(record, runtime, file, search, format, log, config, encode, libreria) {

         var objContext = runtime.getCurrentScript();
         // Parametros del Schedule
         var paramSubsidiaria;
         var paramPeriodo;
         var paramMultibook;
         var paramIdReport;
         var paramIdLog;
         var paramIdFeatureByVersion;
         var paramConcepto;
         // Features del Ambiente
         var hasSubsidiariaFeature;
         var hasMultibookFeature;
         var hasJobFeature;
         var isAdvanceJobsFeature;
         // Datos de la Subsidiaria Seleccinada
         var companyName = null;
         var companyRuc = null;
         var accountsIdArray = new Array;
         var multibookName;
         var CANT_REGISTROS = 85;
         // Datos del Periodo Seleccionado
         var periodEndDate = null;
         var periodStartDate = null;
         var paisesArray = [];
         var LMRY_script = 'LMRY_CO_ReporteMagAnualF1008v7.1_MPRD_V2.0.js';
         var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);
         var GLOBAL_LABELS = {};
         /**
          * Input Data for processing
          *
          * @return Array,Object,Search,File
          *
          * @since 2016.1
          */
         function getInputData() {
             try {

                 GLOBAL_LABELS = getGlobalLabels();
                 ObtenerParametrosYFeatures();
                 ObtenerDatosSubsidiaria();

                 //* SE OBTIENEN LAS CUENTAS CON FORMATO DE MEDIO MAGNETICO
                 accountsIdArray = ObtenerCuentas();
                 log.debug("accountsIdArray", accountsIdArray);

                 //* SE OBTIENEN LAS TRANSACCIONES LLAMANDO A LA BUSQUEDA AÑO POR AÑO HASTA EL 2013 
                 log.debug('Comenzo carga de la busqueda book Specific F')
                 var anioFinal = 2013;
                 var paramAnio = paramPeriodo;
                 var numBucle = paramAnio - anioFinal;
                 //* NUMERO DE VECES QUE SE EJECUTARA LA BUSQUEDA DEL PARAMPERIOD AL 2013
                 log.debug('numBucle ', numBucle);

                 var periodStartDate = new Date(paramAnio, 0, 1);
                 var periodEndDate = new Date(paramAnio, 11, 31);
                 log.debug('paramAnio ', paramAnio);

                 var saldosTotales = new Array;

                 for (var i = 0; i < numBucle; i++) {

                     var vector_saldos = new Array;
                     log.debug('periodStartDate - periodEndDate', periodStartDate + ' - ' + periodEndDate);

                     var periodStartDateFormat = format.format({
                         value: periodStartDate,
                         type: format.Type.DATE
                     });
                     var periodEndDateFormat = format.format({
                         value: periodEndDate,
                         type: format.Type.DATE
                     });
                     vector_saldos = ObtieneSaldos1008(periodStartDateFormat, periodEndDateFormat, true, 'F');
                     log.debug('vector_saldos F ' + paramAnio, vector_saldos.length);

                     saldosTotales = saldosTotales.concat(vector_saldos);

                     paramAnio = paramAnio - 1;
                     //log.debug('paramAnio ', paramAnio);
                     periodStartDate = new Date(paramAnio, 0, 1);
                     periodEndDate = new Date(paramAnio, 11, 31);

                 }
                 log.debug('ULTIMA BUSQUEDA 2013 HASTA INICIOS DE LOS TIEMPOS')
                 log.debug('periodEndDate', periodEndDate);
                 var periodEndDateFormat = format.format({
                     value: periodEndDate,
                     type: format.Type.DATE
                 });
                 vector_saldos = new Array;
                 vector_saldos = ObtieneSaldos1008('', periodEndDateFormat, false, 'F');
                 log.debug('vector_saldos ' + paramAnio, vector_saldos.length);

                 saldosTotales = saldosTotales.concat(vector_saldos);
                 log.debug('saldosTotales BOOK SPECIFIC F ', saldosTotales.length);

                 //* SI TIENE MULTIBOOK, OBTENER TRANSACCIONES CON BOOK SPECIFIC TRUE
                 if (hasMultibookFeature) {
                     log.debug('Comenzo carga de la busqueda book Specific V')
                     periodEndDate = new Date(paramPeriodo, 11, 31);
                     var periodEndDateFormat = format.format({
                         value: periodEndDate,
                         type: format.Type.DATE
                     });

                     vector_saldos = new Array;
                     vector_saldos = ObtieneSaldos1008('', periodEndDateFormat, false, 'T');
                     log.debug('vector_saldos BOOK TRUE', vector_saldos.length);

                     saldosTotales = saldosTotales.concat(vector_saldos);


                 }
                 log.debug('saldosTotales F + T', saldosTotales.length);

                 //* SE SEPARAN LOS ELEMENTOS DEL ARREGLO FINAL EN BLOQUES DE 30 [[30E],[30E],[30E],...]
                 //* PARA QUE DEMORE MENOS EN EJECUTARSE EL MAP

                 var saldosTotalesArray = dividirArray(saldosTotales, 30);
                 log.debug('saldosTotalesArray ', saldosTotalesArray.length);

                 if (saldosTotalesArray.length != 0) {
                     return saldosTotalesArray;
                 } else {
                     NoData('1');
                 }

             } catch (error) {
                 log.error('Error de getInputData', error);
                 return [{
                     "isError": "T",
                     "error": error
                 }];
             }
         }

         /**
          * If this entry point is used, the map function is invoked one time for each key/value.
          *
          * @param {Object} context
          * @param {boolean} context.isRestarted - Indicates whether the current invocation represents a restart
          * @param {number} context.executionNo - Version of the bundle being installed
          * @param {Iterator} context.errors - This param contains a "iterator().each(parameters)" function
          * @param {string} context.key - The key to be processed during the current invocation
          * @param {string} context.value - The value to be processed during the current invocation
          * @param {function} context.write - This data is passed to the reduce stage
          *
          * @since 2016.1
          */
         function map(context) {
             try {
                 ObtenerParametrosYFeatures();
                 var objResult = JSON.parse(context.value);
                 log.debug('MAP objResult', objResult.length);
                 log.debug('objResult', objResult);
                 ObtenerPaises();

                 for (var i = 0; i < objResult.length; i++) {

                     if (objResult[i]["isError"] == "T") {
                         context.write({
                             key: context.key,
                             value: objResult
                         });
                     } else {

                         var accountDetailJson = getTransactionDetail(objResult[i]);

                         if (accountDetailJson != null) {
                             log.debug('accountDetailJson', accountDetailJson);
                             context.write({
                                 key: accountDetailJson[0],
                                 value: {
                                     stringTransaction: accountDetailJson[1]
                                 }
                             });
                         } else {
                             log.debug('NO ENTRA accountDetailJson', accountDetailJson);
                         }


                     }
                 }
             } catch (error) {
                 log.error('Error de Map', error);
                 context.write({
                     key: objResult[0],
                     value: {
                         isError: "T",
                         error: error
                     }
                 });
             }
         }


         /**
          * If this entry point is used, the reduce function is invoked one time for
          * each key and list of values provided..
          *
          * @param {Object} context
          * @param {boolean} context.isRestarted - Indicates whether the current invocation represents a restart
          * @param {number} context.executionNo - Version of the bundle being installed
          * @param {Iterator} context.errors - This param contains a "iterator().each(parameters)" function
          * @param {string} context.key - The key to be processed during the current invocation
          * @param {string} context.value - The value to be processed during the current invocation
          * @param {function} context.write - This data is passed to the reduce stage
          *
          * @since 2016.1
          */
         function reduce(context) {
             try {
                 var vectorMap = [];
                 var row_retenido = [];
                 var sumacred = 0;

                 vectorMap = context.values;
                 // namEmpresa = context.key;
                 for (var j = 0; j < vectorMap.length; j++) {

                     var obj = JSON.parse(vectorMap[j]);

                     if (obj["isError"] == "T") {
                         context.write({
                             key: context.key,
                             value: obj
                         });
                         return;
                     } else {
                         //log.debug('stringtra reduce', obj["stringTransaction"]);
                         row_retenido = obj["stringTransaction"].split('|');
                         sumacred = sumacred + Number(row_retenido[13]);
                     }

                 }

                 var aux_agrupado = row_retenido[12] + '|' + row_retenido[0] + '|' + row_retenido[1] + '|' + row_retenido[2] + '|' + row_retenido[3] + '|' + row_retenido[4] + '|' + row_retenido[5] + '|' + row_retenido[6] + '|' + row_retenido[7] + '|' + row_retenido[8] + '|' + row_retenido[9] + '|' + row_retenido[10] + '|' + row_retenido[11] + '|' + sumacred;
                 log.debug('aux_agrupado reduce', aux_agrupado);

                 if (row_retenido != '') {
                     context.write({
                         key: '1',
                         value: {
                             strRetencionesIva: aux_agrupado
                         }
                     });
                 }

             } catch (error) {
                 log.error('Error de Reduce', error);
                 context.write({
                     key: context.key,
                     value: {
                         isError: "T",
                         error: error
                     }
                 });
             }

         }
         /**
          * If this entry point is used, the reduce function is invoked one time for
          * each key and list of values provided..
          *
          * @param {Object} context
          * @param {boolean} context.isRestarted - Indicates whether the current invocation of the represents a restart.
          * @param {number} context.concurrency - The maximum concurrency number when running the map/reduce script.
          * @param {Date} context.datecreated - The time and day when the script began running.
          * @param {number} context.seconds - The total number of seconds that elapsed during the processing of the script.
          * @param {number} context.usage - TThe total number of usage units consumed during the processing of the script.
          * @param {number} context.yields - The total number of yields that occurred during the processing of the script.
          * @param {Object} context.inputSummary - Object that contains data about the input stage.
          * @param {Object} context.mapSummary - Object that contains data about the map stage.
          * @param {Object} context.reduceSummary - Object that contains data about the reduce stage.
          * @param {Iterator} context.ouput - This param contains a "iterator().each(parameters)" function
          *
          * @since 2016.1
          */
         function summarize(context) {
             try {
                 log.debug('LLego summarize');
                 ObtenerPaises();
                 GLOBAL_LABELS = getGlobalLabels();
                 ObtenerParametrosYFeatures();
                 log.debug('parametros summa', paramSubsidiaria + '-' + paramConcepto);
                 ObtenerDatosSubsidiaria();
                 var vectorReduce = [];
                 var vectorCuantiasMenores = [];
                 var vectorFinal = [];
                 var val_total = 0;
                 //Vector para almacenar vectores de retenciones que se obtuvieron de la funcion reduce
                 //Tam vector_retenciones = cantidad de lineas del reporte
                 var vector_retenciones = [];
                 var errores = [];

                 context.output.iterator().each(function(key, value) {
                     var obj = JSON.parse(value);
                     if (obj["isError"] == "T") {
                         errores.push(JSON.stringify(obj["error"]));
                     } else {

                         vectorReduce = obj.strRetencionesIva.split('|');
                         vector_retenciones.push(vectorReduce);
                     }
                     return true;
                 });
                 for (var i = 0; i < vector_retenciones.length; i++) {
                     //log.debug('vector_retenciones',vector_retenciones[i]);
                     if (Number(Math.abs(vector_retenciones[i][13])) >= Number(CUANTIA_MINIMA)) {
                         vectorFinal.push(vector_retenciones[i]);
                     } else {
                         vectorCuantiasMenores.push(vector_retenciones[i]);
                     }
                 }
                 log.debug('[ summarize ] vectorCuantiasMenores', vectorCuantiasMenores);
                 log.debug('[ summarize ] vectorFinal', vectorFinal);

                 var vectorCuantias = GenerarIngresosPorCuantias(vectorCuantiasMenores, vectorFinal);
                 log.debug('[ summarize ] vectorCuantias', vectorCuantias);
                 // log.debug('vectorFinal' + vectorFinal.length, vectorFinal);
                 for (var j = 0; j < vectorCuantias.length; j++) {
                     val_total = val_total + Number(vectorCuantias[j][13]);
                 }

                 if (errores.length > 0) {
                     // libreria.sendMail(LMRY_script, errores[0]);
                     NoData('2');
                 } else {
                     if (vectorCuantias.length != 0) {
                         var numeroEnvio = obtenerNumeroEnvio();
                         log.debug('numeroEnvio: ', numeroEnvio);
                         GenerarXml(vectorCuantias, val_total.toFixed(0), numeroEnvio);
                         GenerarExcel(vectorCuantias, numeroEnvio);
                     } else {
                         NoData('1');
                     }
                 }

             } catch (error) {
                 libreria.sendMail(LMRY_script, error);
                 NoData('2');
             }
         }

         function dividirArray(arrayData, divisor) {

             var arrayResult = new Array();
             var tam = arrayData.length;
             var ini = 0;
             var fin = divisor

             while (fin <= tam) {
                 var partArray = arrayData.slice(ini, fin);
                 arrayResult.push(partArray);

                 ini = fin;
                 fin += divisor;
             }

             if (ini < tam) {
                 var partArray = arrayData.slice(ini, fin);
                 arrayResult.push(partArray);
             }

             return arrayResult;
         }

         function getGlobalLabels() {
             var labels = {
                 "titulo": {
                     "es": 'FORMULARIO 1008: SALDO DE CUENTAS POR COBRAR',
                     "pt": 'FORMULARIO 1008: SALDO DE CONTAS A RECEBER',
                     "en": 'FORM 1008: BALANCE OF ACCOUNTS RECEIVABLE'
                 },
                 "razonSocial": {
                     "es": 'Razon Social',
                     "pt": 'Razao social',
                     "en": 'Company name'
                 },
                 "taxNumber": {
                     "es": 'Numero de Identificacion Tributaria',
                     "pt": 'Numero de Identificacao Fiscal',
                     "en": 'Tax Number'
                 },
                 "periodo": {
                     "es": 'Periodo',
                     "pt": 'Periodo',
                     "en": 'Period'
                 },
                 "concepto": {
                     "es": 'Concepto',
                     "pt": 'Conceito',
                     "en": 'Concept'
                 },
                 "primerApellido": {
                     "es": '1er Apellido',
                     "pt": '1º Sobrenome',
                     "en": '1st Last Name'
                 },
                 "segApellido": {
                     "es": '2do Apellido',
                     "pt": '2º Sobrenome',
                     "en": '2nd Last Name'
                 },
                 "primerNombre": {
                     "es": '1er Nombre',
                     "pt": '1º nome',
                     "en": '1st Name'
                 },
                 "segNombre": {
                     "es": '2do Nombre',
                     "pt": '2º nome',
                     "en": '2nd Name'
                 },
                 "direccion": {
                     "es": 'Direccion',
                     "pt": 'Endereco',
                     "en": 'Address'
                 },
                 "departamento": {
                     "es": 'Depto',
                     "pt": 'Departamento',
                     "en": 'Department'
                 },
                 "pais": {
                     "es": 'Pais',
                     "pt": 'Pais',
                     "en": 'Country'
                 },
                 "saldos": {
                     "es": 'Saldos',
                     "pt": 'Saldos',
                     "en": 'Balances'
                 },
                 "al": {
                     "es": 'hasta',
                     "pt": 'até',
                     "en": 'until'
                 },
                 'noData1': {
                     "es": 'No existe informacion para los criterios seleccionados.',
                     "pt": 'Não há informações para os critérios selecionados.',
                     "en": 'There is no information for the selected criteria.'
                 },
                 'noData2': {
                     "es": 'Ocurrio un error inesperado en la ejecucion del reporte.',
                     "pt": 'Ocorreu um erro inesperado ao executar o relatório.',
                     "en": 'An unexpected error occurred while executing the report.'
                 },
                 'libroContable': {
                     "es": 'Libro Contable',
                     "pt": 'Livro de Contabilidade',
                     "en": 'Accounting Book'
                 },
                 "origin": {
                    "es": 'Origen',
                    "pt": 'Origem',
                    "en": 'Origin'
                 },
                 "date": {
                    "es": 'Fecha',
                    "pt": 'Data',
                    "en": 'Date'
                 },
                 "time": {
                    "es": 'Hora',
                    "pt": 'Hora',
                    "en": 'Time'
                 }
             }

             return labels;
         }


         function ValidarCaracteres_Especiales(s) {
             var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðòóôõöùúûüýÿ°–—ªº·¢∞¬÷";
             var RegChars = "SZszYAAAAAACEEEEIIIIDOOOOOUUUUYaaaaaaceeeeiiiidooooouuuuyyo--ao.    ";
             s = s.toString();
             for (var c = 0; c < s.length; c++) {
                 for (var special = 0; special < AccChars.length; special++) {
                     if (s.charAt(c) == AccChars.charAt(special)) {
                         s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
                     }
                 }
             }
             return s;
         }

         function GenerarExcel(ingresosRecibidosArray, numeroEnvio) {
             try {
                 var xlsString = '';
                 var periodEndDate = "31/12/" + paramPeriodo;

                 //PDF Normalization
                var todays = parseDateTo(new Date(), "DATE");
                var currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

                 //cabecera de excel
                 xlsString = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
                 xlsString += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
                 xlsString += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
                 xlsString += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
                 xlsString += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
                 xlsString += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
                 xlsString += '<Styles>';
                 xlsString += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
                 xlsString += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/></Style>';
                 xlsString += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/><NumberFormat ss:Format="_(* ###0_);_(* \(###0\);_(@_)"/></Style>';
                 xlsString += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* ###0_);_(* \(###0\);_(@_)"/></Style>';
                 xlsString += '</Styles><Worksheet ss:Name="Sheet1">';

                 xlsString += '<Table>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                 xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';

                 //Cabecera
                 xlsString += '<Row>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['titulo'][language] + ' </Data></Cell>';
                 xlsString += '</Row>';
                 xlsString += '<Row></Row>';
                 xlsString += '<Row>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['razonSocial'][language] + ': ' + companyName + '</Data></Cell>';
                 xlsString += '</Row>';
                 xlsString += '<Row>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['taxNumber'][language] + ': ' + companyRuc + '</Data></Cell>';
                 xlsString += '</Row>';
                 xlsString += '<Row>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['periodo'][language] + ': ' + GLOBAL_LABELS['al'][language] + ' ' + periodEndDate + '</Data></Cell>';
                 xlsString += '</Row>';

                 if (hasMultibookFeature || hasMultibookFeature == 'T') {
                     xlsString += '<Row>';
                     xlsString += '<Cell></Cell>';
                     xlsString += '<Cell></Cell>';
                     xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['libroContable'][language] + ': ' + multibookName + '</Data></Cell>';
                     xlsString += '</Row>';
                 }

                 //PDF Normalized

                 xlsString += '<Row>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['origin'][language] + ': Netsuite' + '</Data></Cell>';
                 xlsString += '</Row>';
                 xlsString += '<Row>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['date'][language] + ': ' + todays + '</Data></Cell>';
                 xlsString += '</Row>';
                 xlsString += '<Row>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell></Cell>';
                 xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['time'][language] + ': ' + currentTime + '</Data></Cell>';
                 xlsString += '</Row>';
                 
                 // END PDF Normalized

                 xlsString += '<Row></Row>';
                 xlsString += '<Row></Row>';
                 xlsString += '<Row>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['concepto'][language] + ' </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> TDOC </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> NID </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> D.V. </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['primerApellido'][language] + ' </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['segApellido'][language] + ' </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['primerNombre'][language] + ' </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['segNombre'][language] + ' </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['razonSocial'][language] + ' </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['direccion'][language] + ' </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['departamento'][language] + ' </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> M/PIO </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['pais'][language] + ' </Data></Cell>' +
                     '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['saldos'][language] + ' CtaxCobrar </Data></Cell>' +
                     '</Row>';

                 //creacion de reporte xls
                 for (var ii = 0; ii < ingresosRecibidosArray.length; ii++) {
                     if (Number(Math.abs(ingresosRecibidosArray[ii][13]).toFixed()) > 0) {

                         xlsString += '<Row>';
                         //0. CONCEPTO
                         if (ingresosRecibidosArray[ii][0] != '' || ingresosRecibidosArray[ii][0] != null) {
                             if (ingresosRecibidosArray[ii][0] != '- None -')
                                 xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][0] + '</Data></Cell>';
                             else
                                 xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }

                         //1. TDOC
                         if (ingresosRecibidosArray[ii][1] != '' || ingresosRecibidosArray[ii][1] != null) {
                             if (ingresosRecibidosArray[ii][1] != '- None -')
                                 xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][1] + '</Data></Cell>';
                             else
                                 xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }

                         //2. NID
                         if (ingresosRecibidosArray[ii][2] != '' || ingresosRecibidosArray[ii][2] != null) {
                             if (ingresosRecibidosArray[ii][2] != '- None -') {
                                 ingresosRecibidosArray[ii][2] = ingresosRecibidosArray[ii][2];
                                 xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][2] + '</Data></Cell>';
                             } else
                                 xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }

                         //3. DV
                         if (ingresosRecibidosArray[ii][3] != '' || ingresosRecibidosArray[ii][3] != null) {
                             if (ingresosRecibidosArray[ii][3] != '- None -') {
                                 ingresosRecibidosArray[ii][3] = ingresosRecibidosArray[ii][3];
                                 xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][3] + '</Data></Cell>';
                             } else
                                 xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }

                         //4. 1ER APELL
                         if (ingresosRecibidosArray[ii][4] && ingresosRecibidosArray[ii][4]) {
                             xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][4] + '</Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }

                         //5. 2DO APELL
                         if (ingresosRecibidosArray[ii][5] && ingresosRecibidosArray[ii][5]) {
                             xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][5] + '</Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }

                         //6. 1ER NOMBRE
                         if (ingresosRecibidosArray[ii][6] && ingresosRecibidosArray[ii][6]) {
                             xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][6] + '</Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }

                         //7. 2DO NOMBRE
                         if (ingresosRecibidosArray[ii][7] && ingresosRecibidosArray[ii][7]) {
                             xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][7] + '</Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }

                         //8. RAZON SOCIAL
                         if (ingresosRecibidosArray[ii][8] != '' || ingresosRecibidosArray[ii][8] != null) {
                             if (ingresosRecibidosArray[ii][8] != '- None -') {
                                 ingresosRecibidosArray[ii][8] = ingresosRecibidosArray[ii][8];
                                 xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][8] + '</Data></Cell>';
                             } else
                                 xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }
                         //9. DIRECCION
                         if (ingresosRecibidosArray[ii][9] != '' || ingresosRecibidosArray[ii][9] != null) {
                             if (ingresosRecibidosArray[ii][9] != '- None -') {
                                 xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][9] + '</Data></Cell>';
                             } else
                                 xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }
                         //10.COD DEPARTAMENTO
                         if (ingresosRecibidosArray[ii][10] != '' || ingresosRecibidosArray[ii][10] != null) {
                             if (ingresosRecibidosArray[ii][10] != '- None -') {
                                 xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][10] + '</Data></Cell>';
                             } else
                                 xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }
                         //11. MUNICIPIO
                         if (ingresosRecibidosArray[ii][11] != '' || ingresosRecibidosArray[ii][11] != null) {
                             if (ingresosRecibidosArray[ii][11] != '- None -') {
                                 xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][11] + '</Data></Cell>';
                             } else
                                 xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }

                         //12. PAIS
                         if (ingresosRecibidosArray[ii][12] != '' || ingresosRecibidosArray[ii][12] != null) {
                             if (ingresosRecibidosArray[ii][12] != '- None -') {
                                 xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][12] + '</Data></Cell>';
                             } else
                                 xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         } else {
                             xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                         }

                         //13. INGRESOS BRUTOS RECIBIDOS POR OPERACIONES PROPIAS
                         if (ingresosRecibidosArray[ii][13] != '' || ingresosRecibidosArray[ii][13] != null) {
                             if (ingresosRecibidosArray[ii][13] != '- None -')
                                 xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(Math.abs(ingresosRecibidosArray[ii][13])).toFixed(0) + '</Data></Cell>';
                             else
                                 xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0</Data></Cell>';
                         } else {
                             xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0</Data></Cell>';
                         }

                         xlsString += '</Row>';
                     }
                 } //fin del quiebre por clase

                 xlsString += '</Table></Worksheet></Workbook>';

                 //Se arma el archivo EXCEL
                 strExcelIngresosRecibidos = encode.convert({
                     string: xlsString,
                     inputEncoding: encode.Encoding.UTF_8,
                     outputEncoding: encode.Encoding.BASE_64
                 });

                 SaveFile('.xls', strExcelIngresosRecibidos, numeroEnvio);
             } catch (error) {
                 log.error('[ERROR EXCEL]', error);
             }

         }

         function completar_cero(long, valor) {
             //log.debug('valor', valor + '-' + ('' + valor).length + '-' + long);

             if ((('' + valor).length) <= long) {
                 //log.debug('valor entra', valor + '-' + valor.length + '-' + long);

                 if (long != ('' + valor).length) {
                     for (var i = (('' + valor).length); i < long; i++) {
                         valor = '0' + valor;
                     }
                 } else {
                     return valor;
                 }
                 return valor;
             } else {
                 //log.debug('valor no entra', valor + '-' + valor.length + '-' + long);

                 valor = valor.substring(0, long);
                 return valor;
             }

         }

         function GenerarXml(vector, valorTotal, numeroEnvio) {

             var xmlString = '';
             var strXmlVentasXPagar = '';
             var cantidadDatos = 0;
             var today = new Date();
             var anio = today.getFullYear();
             log.debug('today' + paramConcepto, today);
             var mes = completar_cero(2, today.getMonth() + 1);
             var day = completar_cero(2, today.getDate());
             var hour = completar_cero(2, today.getHours());
             var min = completar_cero(2, today.getMinutes());
             var sec = completar_cero(2, today.getSeconds());
             today = anio + '-' + mes + '-' + day + 'T' + hour + ':' + min + ':' + sec;

             for (var i = 0; i < vector.length; i++) {
                 //log.debug('vector xml', vector[i]);
                 if (Number(Math.abs(vector[i][13]).toFixed(2)) > 0) {
                     xmlString += '<saldoscc sal="' + Number(Math.abs(vector[i][13])).toFixed(0) + '" pais="' + vector[i][12] + '" mun="' + vector[i][11] + '" dpto="' + vector[i][10] + '" dir="' + vector[i][9].replace(/&/g, '&amp;') + '" raz="' + vector[i][8].replace(/&/g, '&amp;');

                     if (vector[i][7]) {
                         xmlString += '" nomb2="' + vector[i][7].replace(/&/g, '&amp;');
                     } else {
                         xmlString += '" nomb2="';
                     }

                     if (vector[i][6]) {
                         xmlString += '" nomb1="' + vector[i][6].replace(/&/g, '&amp;');
                     } else {
                         xmlString += '" nomb1="';
                     }

                     if (vector[i][5]) {
                         xmlString += '" apl2="' + vector[i][5].replace(/&/g, '&amp;');
                     } else {
                         xmlString += '" apl2="';
                     }

                     if (vector[i][4]) {
                         xmlString += '" apl1="' + vector[i][4].replace(/&/g, '&amp;');
                     } else {
                         xmlString += '" apl1="';
                     }

                     xmlString += '" dv="' + vector[i][3] + '" nid="' + vector[i][2] + '" tdoc="' + vector[i][1] + '" cpt="' + vector[i][0];

                     xmlString += '"/> \r\n';
                     cantidadDatos++;
                 }

             }
             strXmlVentasXPagar += '<?xml version="1.0" encoding="ISO-8859-1"?> \r\n';
             strXmlVentasXPagar += '<mas xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> \r\n';
             strXmlVentasXPagar += '<Cab> \r\n';
             strXmlVentasXPagar += '<Ano>' + paramPeriodo + '</Ano> \r\n';
             strXmlVentasXPagar += '<CodCpt>' + paramConcepto + '</CodCpt> \r\n';
             strXmlVentasXPagar += '<Formato>1008</Formato> \r\n';
             strXmlVentasXPagar += '<Version>71</Version> \r\n';
             strXmlVentasXPagar += '<NumEnvio>' + numeroEnvio + '</NumEnvio> \r\n';
             strXmlVentasXPagar += '<FecEnvio>' + today + '</FecEnvio> \r\n';
             strXmlVentasXPagar += '<FecInicial>' + paramPeriodo + '-01-01</FecInicial> \r\n';
             strXmlVentasXPagar += '<FecFinal>' + paramPeriodo + '-12-31</FecFinal> \r\n';
             strXmlVentasXPagar += '<ValorTotal>' + valorTotal + '</ValorTotal> \r\n';
             strXmlVentasXPagar += '<CantReg>' + cantidadDatos + '</CantReg> \r\n';
             strXmlVentasXPagar += '</Cab>\r\n';
             strXmlVentasXPagar += xmlString;
             strXmlVentasXPagar += '</mas> \r\n';

             //log.debug("strXmlVentasXPagar", strXmlVentasXPagar);

             SaveFile('.xml', strXmlVentasXPagar, numeroEnvio);
         }

         function SaveFile(extension, strArchivo, numeroEnvio) {
             try {
                 var folderId = objContext.getParameter({
                     name: 'custscript_lmry_file_cabinet_rg_co'
                 });

                 var generarXml = false;

                 // Almacena en la carpeta de Archivos Generados
                 if (folderId != '' && folderId != null) {
                     // Extension del archivo
                     var fileName = Name_File(numeroEnvio) + extension;
                     log.debug("fileName", fileName);
                     // Crea el archivo
                     var ventasXPagarFile;

                     if (extension == '.xls') {
                         //log.debug("strExcelVentasXPagar", strArchivo);
                         ventasXPagarFile = file.create({
                             name: fileName,
                             fileType: file.Type.EXCEL,
                             contents: strArchivo,
                             folder: folderId
                         });

                     } else {
                         //log.debug("strXmlVentasXPagar", strArchivo);
                         ventasXPagarFile = file.create({
                             name: fileName,
                             fileType: file.Type.XMLDOC,
                             contents: strArchivo,
                             folder: folderId
                         });

                         generarXml = true;
                     }

                     var fileId = ventasXPagarFile.save();

                     ventasXPagarFile = file.load({
                         id: fileId
                     });

                     var getURL = objContext.getParameter({
                         name: 'custscript_lmry_netsuite_location'
                     });

                     var fileUrl = '';

                     if (getURL != '') {
                         fileUrl += 'https://' + getURL;
                     }

                     fileUrl += ventasXPagarFile.url;
                     log.debug('fileUrl', fileUrl);

                     if (fileId) {

                         if (paramIdReport) {
                             var report = search.lookupFields({
                                 type: 'customrecord_lmry_co_features',
                                 id: paramIdReport,
                                 columns: ['name']
                             });
                             reportName = report.name;
                         }

                         var usuario = runtime.getCurrentUser();
                         var employee = search.lookupFields({
                             type: search.Type.EMPLOYEE,
                             id: usuario.id,
                             columns: ['firstname', 'lastname']
                         });
                         var usuarioName = employee.firstname + ' ' + employee.lastname;
                         log.debug('paramIdLog' + generarXml, usuarioName + '-' + paramIdLog);
                         if (generarXml) {
                             var recordLog = record.create({
                                 type: 'customrecord_lmry_co_rpt_generator_log'
                             });
                         } else {
                             var recordLog = record.load({
                                 type: 'customrecord_lmry_co_rpt_generator_log',
                                 id: paramIdLog
                             });
                         }
                         //Nombre de Archivo
                         recordLog.setValue({
                             fieldId: 'custrecord_lmry_co_rg_name',
                             value: fileName
                         });

                         //Url de Archivo
                         recordLog.setValue({
                             fieldId: 'custrecord_lmry_co_rg_url_file',
                             value: fileUrl
                         });

                         //Nombre de Reporte
                         recordLog.setValue({
                             fieldId: 'custrecord_lmry_co_rg_transaction',
                             value: reportName
                         });

                         //Nombre de Subsidiaria
                         recordLog.setValue({
                             fieldId: 'custrecord_lmry_co_rg_subsidiary',
                             value: companyName
                         });

                         //Periodo
                         recordLog.setValue({
                             fieldId: 'custrecord_lmry_co_rg_postingperiod',
                             value: paramPeriodo
                         });

                         if (hasMultibookFeature) {
                             //Multibook

                             var multibookName = search.lookupFields({
                                 type: search.Type.ACCOUNTING_BOOK,
                                 id: paramMultibook,
                                 columns: ['name']
                             }).name;

                             recordLog.setValue({
                                 fieldId: 'custrecord_lmry_co_rg_multibook',
                                 value: multibookName
                             });
                         }

                         //Creado Por
                         recordLog.setValue({
                             fieldId: 'custrecord_lmry_co_rg_employee',
                             value: usuarioName
                         });

                         recordLog.save();
                         libreria.sendrptuser(reportName, 3, fileName);
                     }
                 } else {
                     log.debug({
                         title: 'Creacion de File:',
                         details: 'No existe el folder'
                     });

                 }
             } catch (error) {
                 log.error('ERROR SAVE', error);
             }


         }

         function Name_File(numeroEnvio) {
             var name = '';
             name = 'Dmuisca_' + completar_cero(2, paramConcepto) + '01008' + '71' + paramPeriodo + completar_cero(8, numeroEnvio);

             return name;
         }

         function NoData(exerror) {
             var usuarioTemp = runtime.getCurrentUser();
             var id = usuarioTemp.id;
             var employeename = search.lookupFields({
                 type: search.Type.EMPLOYEE,
                 id: id,
                 columns: ['firstname', 'lastname']
             });
             var usuario = employeename.firstname + ' ' + employeename.lastname;

             var recordLog = record.load({
                 type: 'customrecord_lmry_co_rpt_generator_log',
                 id: paramIdLog
             });

             switch (exerror) {
                 case '1':
                     var mensaje = GLOBAL_LABELS['noData1'][language];
                     break;
                 case '2':
                     var mensaje = GLOBAL_LABELS['noData2'][language];
                     break;
             }

             //Nombre de Archivo
             recordLog.setValue({
                 fieldId: 'custrecord_lmry_co_rg_name',
                 value: mensaje
             });

             //Creado Por
             recordLog.setValue({
                 fieldId: 'custrecord_lmry_co_rg_employee',
                 value: usuario
             });

             var recordId = recordLog.save();
         }

         function GenerarIngresosPorCuantias(cuantiasMenoresArray, ingresosRecibidosArray) {
             try {

                 var cuantiasAgrupadasArray = [];
                 var recorridos = {};
                 var cuenta, sumaIngBrutos = 0,
                     sumaDevol = 0;
                 var arrayLength = ingresosRecibidosArray.length;
                 var buscarHasta = arrayLength;
                 var dataSubsidiary = getSusbisidiaryRecordById(paramSubsidiaria);
                 var dataCountrySubsidiary = dataSubsidiary[3];
                 var pais_subsidiary = dataCountrySubsidiary[0].value;
                 log.debug('pais_subsidiary', pais_subsidiary);
                 if (pais_subsidiary != '') {
                     if (paisesArray.length > 0) {
                         for (var j = 0; j < paisesArray.length; j++) {
                             if (paisesArray[j][0] == pais_subsidiary) {
                                 pais_subsidiary = paisesArray[j][2];
                             }
                         }
                     } else {
                         pais_subsidiary = "";
                     }
                 } else {
                     pais_subsidiary = "";
                 }

                 for (var i = 0; i < cuantiasMenoresArray.length; i++) {
                     if (cuantiasMenoresArray[i][0] && !recorridos[cuantiasMenoresArray[i][0]]) {
                         cuantiasAgrupadasArray = [];
                         sumaIngBrutos = 0;
                         sumaDevol = 0;
                         recorridos[cuantiasMenoresArray[i][0]] = 1;

                         for (var j = 0; j < cuantiasMenoresArray.length; j++) {
                             if (cuantiasMenoresArray[i][0] == cuantiasMenoresArray[j][0]) {
                                 sumaIngBrutos = sumaIngBrutos + Number(cuantiasMenoresArray[j][13]);
                             }
                         }

                         var id = BuscarIngresoRecibido(cuantiasMenoresArray[i][0], buscarHasta, ingresosRecibidosArray);
                         if (id != -1) {
                             ingresosRecibidosArray[id][13] = Number(ingresosRecibidosArray[id][13]) + sumaIngBrutos;

                         } else {
                             cuantiasAgrupadasArray[0] = cuantiasMenoresArray[i][0];
                             cuantiasAgrupadasArray[1] = '43';
                             cuantiasAgrupadasArray[2] = '222222222';
                             cuantiasAgrupadasArray[3] = '';
                             cuantiasAgrupadasArray[4] = '';
                             cuantiasAgrupadasArray[5] = '';
                             cuantiasAgrupadasArray[6] = '';
                             cuantiasAgrupadasArray[7] = '';
                             cuantiasAgrupadasArray[8] = 'CUANTIAS MENORES';
                             cuantiasAgrupadasArray[9] = dataSubsidiary[0];
                             cuantiasAgrupadasArray[10] = dataSubsidiary[1];
                             cuantiasAgrupadasArray[11] = dataSubsidiary[2];
                             cuantiasAgrupadasArray[12] = pais_subsidiary;
                             cuantiasAgrupadasArray[13] = sumaIngBrutos;

                             ingresosRecibidosArray.push(cuantiasAgrupadasArray);
                             arrayLength++;
                         }

                     }
                 }

                 return ingresosRecibidosArray;
             } catch (e) {
                 log.error('ERROR CUANTIAS MENORES', e);
             }

         }

         function BuscarIngresoRecibido(concepto, buscarHasta, ingresosRecibidosArray) {
             var id = 0;

             for (var i = 0; i < buscarHasta; i++) {
                 if (concepto == ingresosRecibidosArray[i][0] && ingresosRecibidosArray[i][1] == '43' && ingresosRecibidosArray[i][2] == '222222222' && ingresosRecibidosArray[i][8] == 'CUANTIAS MENORES' && ingresosRecibidosArray[i][12] == '169') {
                     return i;
                 }
             }
             return -1;
         }

         function obtenerNumeroEnvio() {

             var numeroLote = 1;
             log.debug('idfeatyre', paramIdFeatureByVersion);
             log.debug('idsubsi', paramSubsidiaria);


             var savedSearch = search.create({
                 type: 'customrecord_lmry_co_lote_rpt_magnetic',
                 filters: [
                     search.createFilter({
                         name: 'internalid',
                         join: 'custrecord_lmry_co_id_magnetic_rpt',
                         operator: search.Operator.IS,
                         values: [paramIdFeatureByVersion]
                     }),
                     search.createFilter({
                         name: 'internalid',
                         join: 'custrecord_lmry_co_subsidiary',
                         operator: search.Operator.IS,
                         values: [paramSubsidiaria]
                     })
                 ],
                 columns: ['internalid', 'custrecord_lmry_co_lote']
             });
             var objResult = savedSearch.run().getRange(0, 1000);

             if (objResult == null || objResult.length == 0) {

                 var loteXRptMgnRecord = record.create({
                     type: 'customrecord_lmry_co_lote_rpt_magnetic'
                 });

                 loteXRptMgnRecord.setValue({
                     fieldId: 'custrecord_lmry_co_id_magnetic_rpt',
                     value: paramIdFeatureByVersion
                 });

                 loteXRptMgnRecord.setValue({
                     fieldId: 'custrecord_lmry_co_year_issue',
                     value: paramPeriodo
                 });

                 loteXRptMgnRecord.setValue({
                     fieldId: 'custrecord_lmry_co_lote',
                     value: numeroLote
                 });

                 loteXRptMgnRecord.setValue({
                     fieldId: 'custrecord_lmry_co_subsidiary',
                     value: paramSubsidiaria
                 })

                 loteXRptMgnRecord.save();

             } else {
                 var columns = objResult[0].columns;
                 var internalId = objResult[0].getValue(columns[0]);
                 numeroLote = Number(objResult[0].getValue(columns[1])) + 1;
                 var loteXRptMgnRecord = record.load({
                     type: 'customrecord_lmry_co_lote_rpt_magnetic',
                     id: internalId
                 });

                 loteXRptMgnRecord.setValue({
                     fieldId: 'custrecord_lmry_co_lote',
                     value: numeroLote
                 });

                 loteXRptMgnRecord.save();
             }

             return numeroLote;
         }

         function ObtieneSaldos1008(periodStartDate, periodEndDate, inbucle, booksspecific) {
             try {

                 var intDMinReg = 0;
                 var intDMaxReg = 1000;
                 var DbolStop = false;

                 var savedSearch = search.load({
                     /*LatamReady - CO Mag1008 Accounts Receivable Balance V7.1*/
                     id: "customsearch_lmry_co_formmag1008_v7"
                 });

                 if (hasSubsidiariaFeature) {
                     var subsidiaryFilter = search.createFilter({
                         name: "subsidiary",
                         operator: search.Operator.IS,
                         values: [paramSubsidiaria]
                     });
                     savedSearch.filters.push(subsidiaryFilter);
                 }

                 if (paramPeriodo) {
                     if (inbucle) {
                         var fechIniFilter = search.createFilter({
                             name: 'trandate',
                             operator: search.Operator.ONORAFTER,
                             values: [periodStartDate]
                         });
                         savedSearch.filters.push(fechIniFilter);
                     }

                     var fechFinFilter = search.createFilter({
                         name: 'trandate',
                         operator: search.Operator.ONORBEFORE,
                         values: [periodEndDate]
                     });
                     savedSearch.filters.push(fechFinFilter);
                 }

                 var vendorColumn = search.createColumn({
                     name: 'formulanumeric',
                     summary: 'GROUP',
                     formula: "CASE WHEN CONCAT ({Type.id},'') = 'ExpRept' THEN {custcol_lmry_exp_rep_vendor_colum.internalid} ELSE CASE WHEN CONCAT ({Type.id},'') = 'Check' THEN {mainname.id} ELSE NVL({vendor.internalid},{vendorline.internalid}) END END"
                 });
                 savedSearch.columns.push(vendorColumn);

                 if (hasJobFeature && !isAdvanceJobsFeature || hasJobFeature && isAdvanceJobsFeature) {
                     log.debug('entra SI jobs', 'SI jobs');
                     var customerColumn = search.createColumn({
                         name: 'formulanumeric',
                         formula: 'CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end',
                         summary: "GROUP"
                     });
                     savedSearch.columns.push(customerColumn);
                 } else {
                     log.debug('entra SIN jobs', 'NO jobs');
                     var customerColumn = search.createColumn({
                         name: "formulanumeric",
                         formula: "{customer.internalid}",
                         summary: "GROUP"
                     });
                     savedSearch.columns.push(customerColumn);
                 }

                 var employeeColumn = search.createColumn({
                     name: 'formulanumeric',
                     summary: 'GROUP',
                     formula: "CASE WHEN CONCAT ({Type.id},'') = 'Journal' or CONCAT ({Type.id},'') = 'Check' or CONCAT ({Type.id},'') = 'ExpRept' THEN {entity.id} ELSE 0 END"
                 });
                 savedSearch.columns.push(employeeColumn);

                 var searchCoumnID = search.createColumn({
                     name: 'formulanumeric',
                     formula: "{internalid}",
                     summary: 'GROUP'
                 });
                 savedSearch.columns.push(searchCoumnID);

                 if (hasMultibookFeature) {

                     var accountFilter = search.createFilter({
                         name: 'account',
                         join: 'accountingtransaction',
                         operator: search.Operator.ANYOF,
                         values: accountsIdArray
                     });
                     // log.debug('accountFilter 1', savedSearch.filters);
                     savedSearch.filters.splice(2, 0, accountFilter);
                     savedSearch.filters.splice(7, 0, accountFilter);
                     savedSearch.filters.splice(12, 0, accountFilter);

                     var multibookFilter = search.createFilter({
                         name: "accountingbook",
                         join: "accountingtransaction",
                         operator: search.Operator.IS,
                         values: [paramMultibook]
                     });
                     savedSearch.filters.push(multibookFilter);

                     var bookSpecificFilter = search.createFilter({
                         name: 'bookspecifictransaction',
                         operator: search.Operator.IS,
                         values: [booksspecific]
                     });
                     savedSearch.filters.push(bookSpecificFilter);

                     ///11. saldo a cobrar.........................17
                     var columSadosXCobrar = search.createColumn({
                         name: 'formulacurrency',
                         formula: "NVL({accountingtransaction.debitamount},'0')-NVL({accountingtransaction.creditamount},'0')",
                         summary: 'SUM'
                     });
                     savedSearch.columns.splice(1, 1, columSadosXCobrar);

                     var searchColumn12 = search.createColumn({
                         name: 'formulanumeric',
                         formula: "{accountingtransaction.account.id}",
                         summary: 'GROUP'
                     });
                     savedSearch.columns.push(searchColumn12);
                     //log.debug('accountFilter filtros', savedSearch.filters);
                     //log.debug('accountFilter columnas', savedSearch.columns);

                 } else {
                     var accountFilter = search.createFilter({
                         name: 'account',
                         operator: search.Operator.ANYOF,
                         values: accountsIdArray
                     });
                     //log.debug('accountFilter filtros', savedSearch.filters);
                     savedSearch.filters.splice(2, 0, accountFilter);
                     savedSearch.filters.splice(7, 0, accountFilter);
                     savedSearch.filters.splice(12, 0, accountFilter);

                     var searchColumn12 = search.createColumn({
                         name: 'formulanumeric',
                         formula: "{account.id}",
                         summary: 'GROUP'
                     });
                     savedSearch.columns.push(searchColumn12);
                 }

                 var searchResult = savedSearch.run();
                 var info2Arr = [];

                 while (!DbolStop) {
                     var objResult = searchResult.getRange(intDMinReg, intDMaxReg);

                     if (objResult != null) {
                         var intLength = objResult.length;
                         if (intLength < 1000) {
                             DbolStop = true;
                         }
                         //log.debug('intLength busqueda', intLength);
                         for (var i = 0; i < intLength; i++) {
                             var Arrtemporal = [];
                             var columns = objResult[i].columns;
                             // 1.- TIPO
                             if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                                 var columna1 = objResult[i].getValue(columns[0]);
                             } else {
                                 var columna1 = '';
                             }
                             // 2.- SALDO A COBRAR
                             if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -')
                                 var columna2 = redondear(objResult[i].getValue(columns[1]));
                             else
                                 var columna2 = 0;

                             // 3.- VENDOR
                             if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                                 var columna3 = objResult[i].getValue(columns[2]);
                             } else {
                                 var columna3 = '';
                             }
                             // 4.- CUSTOMER
                             if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                                 var columna4 = objResult[i].getValue(columns[3]);
                             } else {
                                 var columna4 = '';
                             }
                             // 5.- employee  id
                             if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                                 var columna5 = objResult[i].getValue(columns[4]);
                             } else {
                                 var columna5 = '';
                             }

                             // 6.- internal  id
                             if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                                 var columna7 = objResult[i].getValue(columns[5]);
                             } else {
                                 var columna7 = '';
                             }

                             // 7.- ACCOUNT ID
                             if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                 var columna6 = objResult[i].getValue(columns[6]);
                             } else {
                                 var columna6 = '';
                             }

                             if (columna2 != 0) {
                                 //log.debug('Valores admitidos', columna1 + '-' + columna2 + '-' + columna3 + '-' + columna5 + '-' + columna6 + '-' + columna7);
                                 Arrtemporal = [columna1, columna2, columna3, columna4, columna5, columna6, columna7];
                                 info2Arr.push(Arrtemporal);
                             } else {
                                 log.debug('Valores no admitidos', columna1 + '|' + columna2 + '|' + columna3 + '|' + columna5 + '|' + columna6 + '|' + columna7);
                             }
                         }

                         if (!DbolStop) {
                             intDMinReg = intDMaxReg;
                             intDMaxReg += 1000;
                         }
                     } else {
                         DbolStop = true;
                     }
                 }
                 // log.debug('info2Arr', info2Arr);
                 //log.debug('intLength info2Arr', info2Arr.length);

                 return info2Arr;

             } catch (e) {
                 log.error('[ERROR EN BUSQUEDA]', e);
             }
         }

         function redondear(number) {
             return Math.round(Number(number) * 100) / 100;
         }

         function ObtenerCuentas() {

             var savedSearch = search.create({
                 type: "account",
                 filters: [
                     ["custrecord_lmry_co_puc_formatgy", "anyof", "6"],
                     "AND", ["custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_formatid_c", "is", "1008"]
                 ],
                 columns: [
                     "internalid",
                     "type"
                 ]
             });

             var accountJson = {};
             var objResult = savedSearch.run().getRange(0, 1000);
             var aux = [];
             // log.debug('Cuentas Busqueda', objResult);

             if (objResult && objResult.length) {
                 var columns;
                 for (var i = 0; i < objResult.length; i++) {
                     columns = objResult[i].columns;
                     aux.push(objResult[i].getValue(columns[1]));
                     accountJson[objResult[i].getValue(columns[0])] = aux;
                     aux = [];
                 }
                 var accountsIdArray = Object.keys(accountJson);
                 // log.debug('accountsIdArray', accountsIdArray);
                 return accountsIdArray;
             }

         }

         function ObtenerParametrosYFeatures() {

             paramSubsidiaria = objContext.getParameter({
                 name: "custscript_lmry_subs_co_1008anualv71"
             });
             paramPeriodo = objContext.getParameter({
                 name: "custscript_lmry_period_co_1008anualv71"
             });
             paramMultibook = objContext.getParameter({
                 name: "custscript_lmry_multib_co_1008anualv71"
             });
             paramIdReport = objContext.getParameter({
                 name: "custscript_lmry_feature_co_1008anualv71"
             });
             paramIdLog = objContext.getParameter({
                 name: "custscript_lmry_idlog_co_1008anualv71"
             });
             paramIdFeatureByVersion = objContext.getParameter({
                 name: "custscript_lmry_idfbv_co_1008anualv71"
             });
             paramConcepto = objContext.getParameter({
                 name: "custscript_lmry_concept_co_1008anualv71"
             });

             //Features
             hasSubsidiariaFeature = runtime.isFeatureInEffect({
                 feature: "SUBSIDIARIES"
             });
             //log.debug('subsidiaria', paramConcepto + hasSubsidiariaFeature + '-' + paramSubsidiaria);
             hasMultibookFeature = runtime.isFeatureInEffect({
                 feature: "MULTIBOOK"
             });
             hasJobFeature = runtime.isFeatureInEffect({
                 feature: "JOBS"
             });
             isAdvanceJobsFeature = runtime.isFeatureInEffect({
                 feature: "ADVANCEDJOBS"
             });

             if (paramIdReport) {
                 var recordReport = search.lookupFields({
                     type: "customrecord_lmry_co_features",
                     id: paramIdReport,
                     columns: ["name"]
                 });
                 reportName = recordReport.name;
             }

             if (hasMultibookFeature) {
                 var recordMultibook = search.lookupFields({
                     type: search.Type.ACCOUNTING_BOOK,
                     id: paramMultibook,
                     columns: ["name"]
                 });
                 multibookName = recordMultibook.name;
             }

             if (paramIdFeatureByVersion) {
                 var recordFeatureByVersion = search.lookupFields({
                     type: "customrecord_lmry_co_rpt_feature_version",
                     id: paramIdFeatureByVersion,
                     columns: ["custrecord_lmry_co_amount"]
                 });
                 CUANTIA_MINIMA = recordFeatureByVersion.custrecord_lmry_co_amount;
             }

             log.debug('paramIdFeatureByVersion', paramIdFeatureByVersion);
             log.debug('CUANTIA_MINIMA byversion', CUANTIA_MINIMA);

         }

         function ObtenerDatosSubsidiaria() {

             if (hasSubsidiariaFeature) {
                 companyName = ObtainNameSubsidiaria(paramSubsidiaria);
                 companyRuc = ObtainFederalIdSubsidiaria(paramSubsidiaria);
             } else {
                 var configpage = config.load({
                     type: config.Type.COMPANY_INFORMATION
                 });

                 companyRuc = configpage.getField("employerid");
                 companyName = configpage.getField("legalname");
             }
             companyName = ValidarCaracteres_Especiales(companyName);
             companyRuc = companyRuc.replace(" ", "");
         }

         function ObtainNameSubsidiaria(subsidiary) {
             try {
                 if (subsidiary != "" && subsidiary != null) {
                     var subsidyName = search.lookupFields({
                         type: search.Type.SUBSIDIARY,
                         id: subsidiary,
                         columns: ["legalname"]
                     });


                     return subsidyName.legalname
                 }
             } catch (err) {
                 libreria.sendMail(LMRY_script, " [ ObtainNameSubsidiaria ] " + err);
             }
             return "";
         }

         function ObtenerPaises() {
             var intDMaxReg = 1000;
             var intDMinReg = 0;
             var arrAuxiliar = [];
             var DbolStop = false;

             // Consulta de Cuentas
             var savedSearch = search.load({
                 id: "customsearch_lmry_co_country"
             })

             var searchResult = savedSearch.run();
             var columns;
             while (!DbolStop) {
                 var objResult = searchResult.getRange(intDMinReg, intDMaxReg);

                 if (objResult != null) {
                     var intLength = objResult.length;

                     for (var i = 0; i < intLength; i++) {

                         columns = objResult[i].columns;
                         arrAuxiliar = [];

                         //0. NAME
                         if (objResult[i].getValue(columns[0]) != null)
                             arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                         else
                             arrAuxiliar[0] = "";

                         //1. PAIS
                         if (objResult[i].getValue(columns[1]) != null)
                             arrAuxiliar[1] = objResult[i].getText(columns[1]);
                         else
                             arrAuxiliar[1] = "";

                         //2. CODE
                         if (objResult[i].getValue(columns[2]) != null)
                             arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                         else
                             arrAuxiliar[2] = "";

                         //3. NACIONALIDAD
                         if (objResult[i].getValue(columns[3]) != null)
                             arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                         else
                             arrAuxiliar[3] = "";

                         //4. PAIS LOCALIZACION
                         if (objResult[i].getValue(columns[4]) != null)
                             arrAuxiliar[4] = objResult[i].getValue(columns[4]);
                         else
                             arrAuxiliar[4] = "";

                         paisesArray.push(arrAuxiliar);
                     }
                     intDMinReg = intDMaxReg;
                     intDMaxReg += CANT_REGISTROS;
                     if (intLength < CANT_REGISTROS) {
                         DbolStop = true;
                     }
                 } else {
                     DbolStop = true;
                 }
             }

         }

         function ObtainFederalIdSubsidiaria(subsidiary) {
             try {
                 if (subsidiary != "" && subsidiary != null) {
                     var federalId = search.lookupFields({
                         type: search.Type.SUBSIDIARY,
                         id: subsidiary,
                         columns: ["taxidnum"]
                     });

                     return federalId.taxidnum
                 }
             } catch (err) {
                 libreria.sendMail(LMRY_script, " [ ObtainFederalIdSubsidiaria ] " + err);
             }
             return "";
         }

         function getSusbisidiaryRecordById(subsidiaryId) {

             if (hasSubsidiariaFeature) {
                 var subsidiaryRecord = search.lookupFields({
                     type: search.Type.SUBSIDIARY,
                     id: subsidiaryId,
                     columns: ['address.address1', 'address.custrecord_lmry_addr_city_id', 'address.custrecord_lmry_addr_prov_id', 'address.country']
                 });
             }
             var addressSubsidiary = subsidiaryRecord['address.address1'].replace(/[|]/gi, '').replace(/["]/gi, '');
             addressSubsidiary = ValidarCaracteres_Especiales(addressSubsidiary);
             var provId = subsidiaryRecord['address.custrecord_lmry_addr_prov_id'];
             var cityId = subsidiaryRecord['address.custrecord_lmry_addr_city_id'].substr(-3);
             var countryId = subsidiaryRecord['address.country'];
             return [addressSubsidiary, provId, cityId, countryId];
         }

         function getTransactionDetail(objResult) {
             try {
                 var resultArray = [];
                 var key = '';
                 var valuemap = '';
                 var accountsDetailJson = {};
                 var vectoraux = [];
                 var transactionType = objResult[0];
                 var vendorId = objResult[2];
                 var customerId = objResult[3];
                 var employeeId = objResult[4];

                 log.debug('paramSubsidiaria', paramSubsidiaria);
                 var cuantiamenor = getSusbisidiaryRecordById(paramSubsidiaria);
                 var dataCountrySubsidiary = cuantiamenor[3];
                 var pais_subsidiary = dataCountrySubsidiary[0].value;
                 log.debug('cuantiamenor', cuantiamenor);
                 if (pais_subsidiary != '') {
                     if (paisesArray.length > 0) {
                         for (var j = 0; j < paisesArray.length; j++) {
                             if (paisesArray[j][0] == pais_subsidiary) {
                                 pais_subsidiary = paisesArray[j][2];
                             }
                         }
                     } else {
                         pais_subsidiary = "";
                     }
                 } else {
                     pais_subsidiary = "";
                 }
                 var entidad = '';
                 var internalId = '';

                 if (transactionType == 'FxReval' && vendorId == '' && customerId == '' && employeeId != '0') {
                     var internalIdCurrency = objResult[6];
                     var datos_entidad = ObtenerIdEntidad(internalIdCurrency).split('|');
                     entidad = datos_entidad[0];
                     internalId = datos_entidad[1];
                 } else if (transactionType == 'VendBill' || transactionType == 'VendCred' || transactionType == 'VendPymt' || transactionType == 'CardChrg' || transactionType == 'FxReval' && vendorId != '') {
                     entidad = 'vendor';
                     internalId = vendorId;
                 } else if (transactionType == 'CustInvc' || transactionType == 'CustCred' || transactionType == 'CustPymt' || transactionType == 'CashSale' || transactionType == 'InvAdjst' || transactionType == 'FxReval' && customerId != '') {
                     entidad = 'customer';
                     internalId = customerId;
                 } else {
                     if (vendorId != '') {
                         entidad = 'vendor';
                         internalId = vendorId;
                     } else if (customerId != '') {
                         entidad = 'customer';
                         internalId = customerId;
                     } else if (employeeId != '' && employeeId != '0') {

                         var entitySearchObj = search.create({
                             type: "entity",
                             filters: [
                                 ["internalid", "anyof", employeeId]
                             ],
                             columns: [
                                 search.createColumn({
                                     name: "formulatext",
                                     formula: "{type.id}",
                                     label: "Formula (Text)"
                                 }),
                                 search.createColumn({
                                     name: "internalid",
                                     label: "Internal ID"
                                 })
                             ]
                         });
                         var ObjEntity = entitySearchObj.run().getRange(0, 10);
                         var columns = ObjEntity[0].columns;
                         if (ObjEntity[0].getValue(columns[0]) == 'Vendor') {
                             var entidad = 'vendor';
                         } else if (ObjEntity[0].getValue(columns[0]) == 'CustJob') {
                             var entidad = 'customer';
                         } else if (ObjEntity[0].getValue(columns[0]) == 'Employee') {
                             var entidad = 'employee';
                         }
                         internalId = employeeId;
                     }
                 }

                 //log.debug('[entidad - internalId]: ' +internalId,entidad);

                 var aux_array = [];
                 if (internalId != '' && internalId != 0) {
                     if (transactionType == 'Journal' || transactionType == 'Check' || entidad == 'employee') {
                         var aux_default = getDefaultBillingJournal(internalId);
                         if ((aux_default == 'true' || aux_default == true) && employeeId != '' && entidad == 'employee') {
                             aux_array = getInformationJournal(internalId);
                             resultArray = resultArray.concat(aux_array);
                         } else {
                             if ((aux_default == 'true' || aux_default == true)) {
                                 aux_array = getInformation(entidad, internalId);
                                 resultArray = resultArray.concat(aux_array);
                             } else {
                                 log.debug('[ getTransactionDetail ] lineas omitidas', objResult);
                                 log.debug('[ getTransactionDetail :: getDefaultBillingJournal ] return', aux_default);

                             }
                         }
                     } else {
                         var aux_default = getDefaultBillingJournal(internalId);

                         if ((aux_default == 'true' || aux_default == true)) {
                             aux_array = getInformation(entidad, internalId);
                             resultArray = resultArray.concat(aux_array);
                         } else {
                             log.debug('NO TIENE BILLING', aux_default);
                         }
                     }

                 } else {

                     aux_array = ['43', '222222222', '', '', '', '', '', 'CUANTIAS MENORES', cuantiamenor[0], cuantiamenor[1], cuantiamenor[2], pais_subsidiary];
                     log.debug('se va a cuantias menores', objResult);
                     resultArray = resultArray.concat(aux_array);
                 }

                 //log.debug('resultArray'+transactionType+'-'+internalId,resultArray);

                 if (resultArray.length != 0) {
                     // log.debug('conceptos id', objResult[5]);
                     var id_cuenta = Number(objResult[5]);
                     var concepto = ObtenerConceptoAccount(id_cuenta);

                     resultArray[12] = Number(concepto);
                     resultArray[13] = Number(objResult[1]);
                     // log.debug('conceptos final'+transactionType, objResult[5] + '--  ' + concepto+'--'+resultArray[13]);
                     key = resultArray[0] + "|" + resultArray[1] + "|" + resultArray[2] + "|" + resultArray[3] + "|" + resultArray[4] + "|" + resultArray[5] + "|" + resultArray[6] + "|" + resultArray[7] + "|" + resultArray[8] + "|" + resultArray[9] + "|" + resultArray[10] + "|" + resultArray[11] + "|" + resultArray[12];
                     valuemap = resultArray[0] + "|" + resultArray[1] + "|" + resultArray[2] + "|" + resultArray[3] + "|" + resultArray[4] + "|" + resultArray[5] + "|" + resultArray[6] + "|" + resultArray[7] + "|" + resultArray[8] + "|" + resultArray[9] + "|" + resultArray[10] + "|" + resultArray[11] + "|" + resultArray[12] + "|" + resultArray[13];

                     vectoraux[0] = key;
                     vectoraux[1] = valuemap;
                     //log.debug('key' + key, valuemap);
                     accountsDetailJson[key] = resultArray;

                     return vectoraux;
                 }
             } catch (e) {
                 log.error('ERROR EN ENTIDAD', e);
                 log.error('ERROR objResult', objResult);
             }

         }

         function ObtenerConceptoAccount(idAccount) {
             try {
                 var concepto_enviar = '';
                 var accountSearchObj = search.create({
                     type: "account",
                     filters: [
                         ["internalidnumber", "equalto", idAccount],
                         "AND", ["custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_formatid_c", "is", "1008"]
                     ],
                     columns: [
                         search.createColumn({
                             name: "custrecord_lmry_co_puc_number_c",
                             join: "CUSTRECORD_LMRY_CO_PUC_CONCEPT"
                         })
                     ]
                 });
                 var objResult = accountSearchObj.run().getRange(0, 1);
                 if (objResult && objResult.length) {
                     var columns = objResult[0].columns;

                     // 1. Concepto
                     if (objResult[0].getValue(columns[0])) {
                         concepto_enviar = objResult[0].getValue(columns[0]);
                     } else {
                         concepto_enviar = '';
                     }
                 }

                 return concepto_enviar;
             } catch (e) {
                 log.error('[ERROR EN OBTENER CONCEPTO]', e);
             }

         }

         function ObtenerIdEntidad(idCurrency) {
             var auxArray = [];

             var fxrevalSearchObj = search.create({
                 type: "fxreval",
                 filters: [
                     ["type", "anyof", "FxReval"],
                     "AND", ["internalidnumber", "equalto", idCurrency],
                     "AND", ["formulatext: CASE WHEN NVL({name},'NULL')='NULL' THEN 1 ELSE 0 END", "is", "0"]
                 ],
                 columns: [
                     search.createColumn({
                         name: "formulatext",
                         formula: "{vendorline.internalid}"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "{customer.internalid}"
                     })
                 ]
             });

             var objResult = fxrevalSearchObj.run().getRange(0, 1);
             if (objResult && objResult.length) {
                 var columns = objResult[0].columns;

                 // Vendor
                 if (objResult[0].getValue(columns[0])) {
                     auxArray[0] = objResult[0].getValue(columns[0]);
                 } else {
                     auxArray[0] = '';
                 }

                 // Customer
                 if (objResult[0].getValue(columns[1])) {
                     auxArray[1] = objResult[0].getValue(columns[1]);
                 } else {
                     auxArray[1] = '';
                 }
                 if (auxArray[0] != '') {
                     var entidad = 'vendor';
                     var id_currency_entidad = auxArray[0];
                 } else {
                     var entidad = 'customer';
                     var id_currency_entidad = auxArray[1];
                 }


             } else {
                 log.debug("[ERROR EN ObtenerIdEntidad]", 'no tiene entidad');
             }
             // log.debug('datos customer', auxArray);

             return entidad + '|' + id_currency_entidad;

         }

         function getInformationJournal(internalId) {
             var auxArray = [];

             var employeename = search.lookupFields({
                 type: search.Type.EMPLOYEE,
                 id: internalId,
                 columns: ['custentity_lmry_sunat_tipo_doc_cod', 'custentity_lmry_country', 'custentity_lmry_sv_taxpayer_number', 'firstname', 'lastname', 'address.address1', 'address.custrecord_lmry_addr_prov_id', 'address.custrecord_lmry_addr_city_id', 'address.country']
             });
             var tipo_doc_employee = employeename.custentity_lmry_sunat_tipo_doc_cod;
             var pais_employee = employeename['address.country'];
             pais_employee = pais_employee[0].text;
             var firstname = employeename.firstname;
             var lastname = employeename.lastname;
             var direccion_employee = employeename['address.address1'];
             var departamento_employee = employeename['address.custrecord_lmry_addr_prov_id'];
             var municipality_employee = employeename['address.custrecord_lmry_addr_city_id'].substr(-3);
             log.debug('firstname', firstname + '-' + lastname);

             if (pais_employee != '') {
                 if (paisesArray.length > 0) {
                     for (var j = 0; j < paisesArray.length; j++) {
                         if (paisesArray[j][0] == pais_employee) {
                             pais_employee = paisesArray[j][2];
                         }
                     }
                 } else {
                     pais_employee = "";
                 }
             } else {
                 pais_employee = "";
             }


             var vatregnumber_employee = employeename.custentity_lmry_sv_taxpayer_number;
             // var name_employee = employeename.name;

             auxArray[0] = tipo_doc_employee;
             auxArray[1] = QuitarCaracteres(vatregnumber_employee);
             auxArray[2] = ''; //DV

             if (firstname.split(' ').length > 1) {
                 auxArray[3] = firstname.split(' ')[0];
                 auxArray[4] = firstname.split(' ')[1];
             } else {
                 auxArray[3] = firstname.split(' ')[0];
                 auxArray[4] = '';
             }
             auxArray[4] = auxArray[4].replace(/[|]/gi, '');
             auxArray[4] = auxArray[4].replace(/["]/gi, '');
             auxArray[4] = ValidarCaracteres_Especiales(auxArray[4]);
             auxArray[3] = auxArray[3].replace(/[|]/gi, '');
             auxArray[3] = auxArray[3].replace(/["]/gi, '');
             auxArray[3] = ValidarCaracteres_Especiales(auxArray[3]);

             if (lastname.split(' ').length > 1) {
                 auxArray[5] = lastname.split(' ')[0];
                 auxArray[6] = lastname.split(' ')[1];
             } else {
                 auxArray[5] = lastname.split(' ')[0];
                 auxArray[6] = '';
             }

             auxArray[6] = auxArray[6].replace(/[|]/gi, '');
             auxArray[6] = auxArray[6].replace(/["]/gi, '');
             auxArray[6] = ValidarCaracteres_Especiales(auxArray[6]);
             auxArray[5] = auxArray[5].replace(/[|]/gi, '');
             auxArray[5] = auxArray[5].replace(/["]/gi, '');
             auxArray[5] = ValidarCaracteres_Especiales(auxArray[5]);

             auxArray[7] = '';
             auxArray[8] = direccion_employee.replace(/[|]/gi, '').replace(/["]/gi, '');
             auxArray[8] = ValidarCaracteres_Especiales(auxArray[8]);
             auxArray[9] = departamento_employee;
             auxArray[10] = municipality_employee;
             auxArray[11] = pais_employee;
             log.debug('EMPLOYEE ', auxArray);
             return auxArray;

         }

         function getDefaultBillingJournal(internalId) {
             var isBilling = false;

             var entitySearchObj = search.create({
                 type: "entity",
                 filters: [
                     ["internalid", "anyof", internalId],
                     "AND", ["isdefaultbilling", "is", "T"]
                 ],
                 columns: [
                     search.createColumn({
                         name: "altname",
                         label: "Name"
                     }),
                     search.createColumn({
                         name: "isdefaultbilling",
                         label: "Default Billing Address"
                     })
                 ]
             });

             entitySearchObj.run().each(function(result) {
                 // .run().each has a limit of 4,000 results
                 isBilling = result.getValue('isdefaultbilling');
                 return true;
             });

             return isBilling;
         }

         function getInformation(entidad, internalId) {

             var auxArray = [];

             if (entidad != '' && internalId != '') {
                 var newSearch = search.create({
                     type: entidad,
                     filters: [
                         ['internalid', 'is', internalId], 'AND', ['isdefaultbilling', 'is', 'T']
                     ],
                     columns: [
                         search.createColumn({
                             name: "formulatext",
                             formula: "{custentity_lmry_sunat_tipo_doc_cod}",
                             label: "0. Tipo de Documento"
                         }),
                         search.createColumn({
                             name: "formulatext",
                             formula: "{vatregnumber}",
                             label: "1. NIT"
                         }),
                         search.createColumn({
                             name: "formulatext",
                             formula: "CASE WHEN {custentity_lmry_sunat_tipo_doc_id} = 'NIT' THEN {custentity_lmry_digito_verificator} ELSE '' END",
                             label: "2. DV"
                         }),
                         search.createColumn({
                             name: "formulatext",
                             formula: "CASE WHEN {isperson} = 'T' THEN {lastname} ELSE '' END",
                             label: "3. Apellidos"
                         }),
                         search.createColumn({
                             name: "formulatext",
                             formula: "CASE WHEN {isperson} = 'T' THEN {firstname} ELSE '' END",
                             label: "4. Nombres"
                         }),
                         search.createColumn({
                             name: "formulatext",
                             formula: "CASE WHEN {isperson} = 'F' THEN {companyname} ELSE '' END",
                             label: "5. Razón Social"
                         }),
                         search.createColumn({
                             name: "formulatext",
                             formula: "{address1}",
                             label: "6. Direccion"
                         }),
                         search.createColumn({
                             name: "formulatext",
                             formula: "SUBSTR({custentity_lmry_municcode}, 0, 2)",
                             label: "7. Cod Dep"
                         }),
                         search.createColumn({
                             name: "formulatext",
                             formula: "SUBSTR({custentity_lmry_municcode}, 3, 3)",
                             label: "8. Municipio"
                         }),
                         search.createColumn({
                             name: "formulatext",
                             formula: "{custentity_lmry_country}",
                             label: "9. PAIS"
                         }),
                     ]
                 });

                 var objResult = newSearch.run().getRange(0, 1000);
                 if (objResult && objResult.length) {
                     var columns = objResult[0].columns;
                     // 0. Tipo de Documento
                     auxArray[0] = objResult[0].getValue(columns[0]);
                     if (auxArray[0] == '') {
                         auxArray[0] = '0';
                     }

                     // 1. NIT
                     auxArray[1] = QuitarCaracteres(objResult[0].getValue(columns[1]));

                     // 2. DV
                     auxArray[2] = objResult[0].getValue(columns[2]);

                     // 3. Apellido Paterno
                     if (objResult[0].getValue(columns[3]).split(' ')[0]) {
                         auxArray[3] = objResult[0].getValue(columns[3]).split(' ')[0];
                     } else {
                         auxArray[3] = '';
                     }
                     auxArray[3] = auxArray[3].replace(/[|]/gi, '');
                     auxArray[3] = auxArray[3].replace(/["]/gi, '');
                     auxArray[3] = ValidarCaracteres_Especiales(auxArray[3]);

                     // 4. Apellido Materno
                     if (objResult[0].getValue(columns[3]).split(' ')[1]) {
                         auxArray[4] = objResult[0].getValue(columns[3]).split(' ')[1];
                     } else {
                         auxArray[4] = '';
                     }
                     auxArray[4] = auxArray[4].replace(/[|]/gi, '');
                     auxArray[4] = auxArray[4].replace(/["]/gi, '');
                     auxArray[4] = ValidarCaracteres_Especiales(auxArray[4]);

                     // 5. Primer Nombre
                     if (objResult[0].getValue(columns[4]).split(' ')[0]) {
                         auxArray[5] = objResult[0].getValue(columns[4]).split(' ')[0];
                     } else {
                         auxArray[5] = '';
                     }
                     auxArray[5] = auxArray[5].replace(/[|]/gi, '');
                     auxArray[5] = auxArray[5].replace(/["]/gi, '');
                     auxArray[5] = ValidarCaracteres_Especiales(auxArray[5]);

                     // 6. Segundo Nombre
                     if (objResult[0].getValue(columns[4]).split(' ')[1]) {
                         auxArray[6] = objResult[0].getValue(columns[4]).split(' ')[1];
                     } else {
                         auxArray[6] = '';
                     }
                     auxArray[6] = auxArray[6].replace(/[|]/gi, '');
                     auxArray[6] = auxArray[6].replace(/["]/gi, '');
                     auxArray[6] = ValidarCaracteres_Especiales(auxArray[6]);

                     // 7. Razón Social
                     auxArray[7] = objResult[0].getValue(columns[5]);
                     auxArray[7] = auxArray[7].replace(/[|]/gi, '');
                     auxArray[7] = auxArray[7].replace(/["]/gi, '');
                     auxArray[7] = ValidarCaracteres_Especiales(auxArray[7]);

                     //8. Direccion
                     auxArray[8] = objResult[0].getValue(columns[6]);
                     auxArray[8] = auxArray[8].replace(/[|]/gi, '');
                     auxArray[8] = auxArray[8].replace(/["]/gi, '');
                     auxArray[8] = ValidarCaracteres_Especiales(auxArray[8]);

                     // 9. Cod Dep
                     auxArray[9] = objResult[0].getValue(columns[7]);

                     // 10. Municipio
                     auxArray[10] = objResult[0].getValue(columns[8]);

                     // 11. Pais
                     auxArray[11] = objResult[0].getValue(columns[9]);
                     if (auxArray[11] != '') {
                         if (paisesArray.length > 0) {
                             for (var j = 0; j < paisesArray.length; j++) {
                                 if (paisesArray[j][0] == auxArray[11]) {
                                     auxArray[11] = paisesArray[j][2];
                                 }
                             }
                         } else {
                             auxArray[11] = "";
                         }
                     } else {
                         auxArray[11] = "";
                     }



                 } else {
                     log.debug("entidad no entra 2", entidad);
                     log.debug("internalId no entra 2", internalId);
                 }
                 // log.debug('datos customer', auxArray);
                 return auxArray;
             } else {
                 log.debug("entidad no entra 1", entidad);
                 log.debug("internalId no entra 1", internalId);
                 return [];
             }

         }

         function QuitarCaracteres(str) {

             var nit = '';
             for (var i = 0; i < str.length; i++) {
                 if (isInteger(Number(str[i])) && str[i] != ' ') {
                     nit += str[i];
                 }
             }
             return nit;
         }

         function isInteger(numero) {
             if (numero % 1 == 0) {
                 return true;
             } else {
                 return false;
             }
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
        
        //** Function used to Get Current Time by only DAYTIME*/
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
             getInputData: getInputData,
             map: map,
             reduce: reduce,
             summarize: summarize
         };

     });