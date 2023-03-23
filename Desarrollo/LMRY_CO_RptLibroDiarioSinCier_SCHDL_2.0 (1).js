/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||               This script for customer center (Time)             ||
||                                                                  ||
||  File Name: LMRY_CO_RptLibroDiarioSinCier_SCHDL_2.0.js           ||
||                                                                  ||
||  Version   Date         Author        Remarks                    ||
||  2.0     Oct 5 2021    Latamready    Use Script 2.0              ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */


/**
 * @NApiVersion 2.0
 * @NScriptType ScheduledScript
 * @NModuleScope Public
 */
define(["N/record", "N/runtime", "N/file", "N/search", "N/format",
        "N/log", "N/config", "N/task", "N/encode", "N/url", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js",
        "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_libSendingEmailsLBRY_V2.0.js"
    ],

    function(recordModulo, runtime, file, search, format, log, config, task, encode, url, libreria, libFeature) {

        /**
         * Definition of the Scheduled script trigger point.
         *
         * @param {Object} scriptContext
         * @param {string} scriptContext.type - The context in which the script is executed. It is one of the values from the scriptContext.InvocationType enum.
         * @Since 2015.2
         */

        var objContext = runtime.getCurrentScript();
        var LMRY_script = 'LMRY CO Reportes Libro Diario sin cierre SCHDL 2.0';
        // Nombre del Reporte
        var namereport = "Reporte de Libro Diario sin cierre";

        //parametros
        var paramsubsidi = '';
        var paramperiodo = '';
        var paramMultibook = null;
        var paramEndPeriod = '';
        //Features
        //Valida si es OneWorld
        var featuresubs = runtime.isFeatureInEffect({
            feature: 'SUBSIDIARIES'
        });;
        var feamultibook = runtime.isFeatureInEffect({
            feature: "MULTIBOOK"
        });
        var featureMultipCalendars = runtime.isFeatureInEffect({
            feature: 'MULTIPLECALENDARS'
        });

        // Control de reporte
        var monthStartD;
        var yearStartD;

        var periodstartdate = '';
        var periodenddate = '';
        var companyruc = '';
        var companyname = '';
        var calendarSubsi = null;
        var taxCalendarSubsi = null;
        var xlsString = '';
        var arrLibroDiario = new Array();
        var arrAccountingContext = new Array();
        var strName = '';
        var periodname = '';
        var auxmess = '';
        var auxanio = '';
        var multibook_name = '';
        //idioma
        var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);
        var GLOBAL_LABELS = getGlobalLabels();

        var RecordName = 'customrecord_lmry_co_rpt_generator_log';
        var RecordTable = ['custrecord_lmry_co_rg_name',
            'custrecord_lmry_co_rg_postingperiod',
            'custrecord_lmry_co_rg_subsidiary',
            'custrecord_lmry_co_rg_url_file',
            'custrecord_lmry_co_rg_employee',
            'custrecord_lmry_co_rg_multibook'
        ];

        //PDF Normalization
        var todays = "";
        var currentTime = "";

        function execute(scriptContext) {
            try {
                ObtenerParametrosYFeatures();

                ObtenerDatosSubsidiaria();

                obtenerPeriodosEspeciales(paramperiodo);

                var feamultibook = runtime.isFeatureInEffect({
                    feature: "MULTIBOOK"
                });

                if (feamultibook == true || feamultibook == 'T') {
                    //hacer un lookupfiel
                    var columna = search.lookupFields({
                        type: 'accountingbook',
                        id: paramMultibook,
                        columns: ['name']
                    });
                    multibook_name = columna.name;
                }

                //Obtiene Accounting Context Funcion
                if (paramMultibook != '1') {
                    var array_context = ObtieneAccountingContext();
                }

                ObtieneLibroDiario();

                if (paramMultibook != 1) {
                    CambioDeCuentas();
                }

                /* if (arrLibroDiario.length != 0) {
                  arrLibroDiario = OrdenarCuentas();
                } */

                todays = parseDateTo(new Date(), "DATE");
                currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

                obtenerExcel()

            } catch (err) {
                log.error({
                    title: 'Se genero un error :',
                    details: err
                });
                return true;
            }

        }

        function obtenerExcel() {

            if (arrLibroDiario.length != null && arrLibroDiario.length != 0) {
                //cabecera del excel
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
                xlsString += '<Worksheet ss:Name="Libro Diario sin Cierre Anual">';

                xlsString += '<Table>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="060"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="220"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="140"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
                //Cabecera
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["Alert1"][language] + ' </Data></Cell>';
                xlsString += '</Row>';
                xlsString += '<Row></Row>';
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["Alert2"][language] + companyname + '</Data></Cell>';
                xlsString += '</Row>';
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">NIT: ' + companyruc + '</Data></Cell>';
                xlsString += '</Row>';
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["Alert3"][language] + periodstartdate + ' al ' + periodenddate + '</Data></Cell>';
                xlsString += '</Row>';
                if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {
                    xlsString += '<Row>';
                    xlsString += '<Cell></Cell>';
                    xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["Alert4"][language] + multibook_name + '</Data></Cell>';
                    xlsString += '</Row>';
                }

                //Normalize PDF
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["origin"][language] + "Netsuite" + '</Data></Cell>';
                xlsString += '</Row>';
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["date"][language] + todays + '</Data></Cell>';
                xlsString += '</Row>';
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["time"][language] + currentTime + '</Data></Cell>';
                xlsString += '</Row>';
                // End Normalize PDF

                // Una linea en blanco
                xlsString += '<Row></Row>';
                // Titulo de las columnas
                xlsString += '<Row></Row>';
                xlsString += '<Row>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert5"][language] + '</Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert6"][language] + '</Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert7"][language] + '</Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert8"][language] + '</Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert9"][language] + '</Data></Cell>' +
                    '</Row>';


                //Creacion de reporte xls
                var fechaActual = arrLibroDiario[0][0];
                var primerDia = arrLibroDiario[0][0];
                var sumDebito = 0.00;
                var sumCredito = 0.00;
                var sumTotDebito = 0.00;
                var sumTotCredito = 0.00;
                var flag = 0; // Esto es para que solo se imprima 1 vez en el primer dia
                var existePrimeraCabecera = false;

                ;
                for (var i = 0; i <= arrLibroDiario.length - 1; i++) {
                    var primeraCabecera = arrLibroDiario[i][0] === primerDia && flag === 0 && (Number(arrLibroDiario[i][3]) != 0 || Number(arrLibroDiario[i][4]) != 0);

                    if (primeraCabecera) {
                        // MergeAcross = "1" (Junta dos columnas) "2" (Junta tres columnas), junta una mas del parametro que se pasa
                        xlsString += '<Row>';
                        xlsString += '<Cell ss:StyleID="s22" ss:MergeAcross="1"><Data ss:Type="String">' + GLOBAL_LABELS["Alert10"][language] + fechaActual + '</Data></Cell>';
                        xlsString += '</Row>'
                        flag++;
                        existePrimeraCabecera = true;
                    }

                    if (fechaActual != arrLibroDiario[i][0] && (Number(arrLibroDiario[i][3]) != 0 || Number(arrLibroDiario[i][4]) != 0)) {
                        if (existePrimeraCabecera || fechaActual != primerDia) {
                            //arma el total de los quiebres
                            xlsString += '<Row>';
                            xlsString += '<Cell></Cell>';
                            xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="String">' + GLOBAL_LABELS["Alert11"][language] + fechaActual + '</Data></Cell>';
                            xlsString += '<Cell></Cell>';
                            xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(sumDebito).toFixed(2) + '</Data></Cell>';
                            xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(sumCredito).toFixed(2) + '</Data></Cell>';
                            xlsString += '</Row>';
                            existePrimeraCabecera = false;
                        }

                        fechaActual = arrLibroDiario[i][0];
                        sumCredito = 0.0;
                        sumDebito = 0.0;

                        //Nuevo quiebre
                        // MergeAcross = "1" (Junta dos columnas) "2" (Junta tres columnas), junta una mas del parametro que se pasa
                        xlsString += '<Row>';
                        xlsString += '<Cell ss:StyleID="s22" ss:MergeAcross="1"><Data ss:Type="String">' + GLOBAL_LABELS["Alert10"][language] + fechaActual + '</Data></Cell>';
                        xlsString += '</Row>';
                    }
                    if ((Number(arrLibroDiario[i][3]) != 0 || Number(arrLibroDiario[i][4]) != 0)) {
                        xlsString += '<Row>';
                        //1. Numero de Cuenta
                        if (arrLibroDiario[i][1] != '' || arrLibroDiario[i][1] != null) {
                            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arrLibroDiario[i][1] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
                        }
                        //2. Denominaci?n
                        if (arrLibroDiario[i][2].length > 0) {
                            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arrLibroDiario[i][2] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
                        }
                        //3. Documento
                        if (arrLibroDiario[i][7] != '' || arrLibroDiario[i][7] != null) {
                            xlsString += '<Cell><Data ss:Type="String">' + arrLibroDiario[i][7] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }
                        //4. Suma Debito
                        if (arrLibroDiario[i][3] != '' || arrLibroDiario[i][3] != null) {
                            xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Number(arrLibroDiario[i][3]) + '</Data></Cell>';
                            sumDebito += parseFloat(arrLibroDiario[i][3]);
                            sumTotDebito += parseFloat(arrLibroDiario[i][3]);
                        } else {
                            xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0.00</Data></Cell>';
                        }
                        //5. Suma Credito
                        if (arrLibroDiario[i][4] != '' || arrLibroDiario[i][4] != null) {
                            xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Number(arrLibroDiario[i][4]) + '</Data></Cell>';
                            sumCredito += parseFloat(arrLibroDiario[i][4]);
                            sumTotCredito += parseFloat(arrLibroDiario[i][4]);
                        } else {
                            xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0.00</Data></Cell>';
                        }
                        xlsString += '</Row>';
                        log.error('registro ' + i, arrLibroDiario[i][0] + ',' + arrLibroDiario[i][1] + ',' + arrLibroDiario[i][2] + ',' + arrLibroDiario[i][3] + ',' + arrLibroDiario[i][4]);
                    }
                }
                //arma el total del ultimo quiebre
                xlsString += '<Row>';
                xlsString += '<Cell ss:StyleID="s23" ss:MergeAcross="1"><Data ss:Type="String">' + GLOBAL_LABELS["Alert11"][language] + fechaActual + '</Data></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(sumDebito).toFixed(2) + '</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(sumCredito).toFixed(2) + '</Data></Cell>';
                xlsString += '</Row>';

                //arma el total del periodo
                xlsString += '<Row>';

                if (language == 'es') {
                    xlsString += '<Cell ss:MergeAcross="1" ss:StyleID="s23"><Data ss:Type="String">' + GLOBAL_LABELS["Alert12"][language] + periodstartdate + ' al ' + periodenddate + '</Data></Cell>';
                } else if (language == 'pt') {
                    xlsString += '<Cell ss:MergeAcross="1" ss:StyleID="s23"><Data ss:Type="String">' + GLOBAL_LABELS["Alert12"][language] + periodstartdate + ' a ' + periodenddate + '</Data></Cell>';
                } else if (language == 'en') {
                    xlsString += '<Cell ss:MergeAcross="1" ss:StyleID="s23"><Data ss:Type="String">' + GLOBAL_LABELS["Alert12"][language] + periodstartdate + ' to ' + periodenddate + '</Data></Cell>';
                }
                //xlsString += '<Cell ss:MergeAcross="1" ss:StyleID="s23"><Data ss:Type="String">' + GLOBAL_LABELS["Alert12"][language] + periodstartdate + ' al ' + periodenddate + '</Data></Cell>';
                xlsString += '<Cell></Cell>';
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
                //strName = nlapiEncrypt(xlsString, 'base64');

                if (paramMultibook != '' && paramMultibook != null) {
                    var NameFile = "COLibroDiarioSinCierreAnual_" + companyname + "_" + monthStartD + "_" + yearStartD + "_" + paramMultibook + ".xls";
                } else {
                    var NameFile = "COLibroDiarioSinCierreAnual_" + companyname + "_" + monthStartD + "_" + yearStartD + ".xls";
                }
                log.debug('auxanio', auxanio);
                savefile(NameFile, 'EXCEL');
            } else {
                var usuarioTemp = runtime.getCurrentUser(); //1.0 -> var usuario = objContext.getName();
                var usuario = usuarioTemp.name;
                //log.debug('objContext', usuario);
                if (paramidrpt != null && paramidrpt != '') {
                    //var record = nlapiLoadRecord(RecordName, paramidrpt); // generator_log
                    var record = recordModulo.load({
                        type: RecordName,
                        id: paramidrpt
                    });

                    record.setValue(RecordTable[0], GLOBAL_LABELS["Alert13"][language]);

                    //record.setValue(RecordTable[0], 'No existe informacion para los criterios seleccionados'); // name


                    //record.setValue(RecordTable[1], periodname); // postingperiod
                    //record.setValue(RecordTable[2], companyname); // subsidiary
                    //record.setValue(RecordTable[4], usuario); // employee
                    //record.setValue(RecordTable[5], multibook_name); // multi

                    record.save();
                    //nlapiSubmitRecord(record, true);

                    libreria.sendrptuser(NameFile);
                }
            }
        }

        function obtenerPeriodosEspeciales(paramperiod) {
            //valida si el accounting special ...
            var licenses = libFeature.getLicenses(paramsubsidi);
            featAccountingSpecial = libFeature.getAuthorization(677, licenses); //true o false, 677

            if (featAccountingSpecial || featAccountingSpecial == 'T') {
                var searchSpecialPeriod = search.create({
                    type: "customrecord_lmry_special_accountperiod",
                    filters: [
                        ["isinactive", "is", "F"],
                        'AND', ["custrecord_lmry_accounting_period", "is", paramperiod]
                    ],
                    columns: [
                        // search.createColumn({
                        //     name: "custrecord_lmry_calendar",
                        //     label: "0. Latam - Calendar"
                        // }),
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

                log.debug('calendarSubsi', calendarSubsi)

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

                        log.debug('periodstartdate', periodstartdate)
                        log.debug('periodenddate', periodenddate)
                        log.debug('periodname', periodname)

                    })
                });
            } else {
                if (paramperiodo != null && paramperiodo != '') {
                    var columnFrom = search.lookupFields({
                        type: 'accountingperiod',
                        id: paramperiodo,
                        columns: ['enddate', 'periodname', 'startdate']
                    });
                    log.debug('paramperiodo', paramperiodo)
                    log.debug('lookupfield', columnFrom)
                    periodstartdate = columnFrom.startdate;
                    periodenddate = columnFrom.enddate;
                    periodname = columnFrom.periodname;

                    log.debug('periodstartdate', periodstartdate)
                    log.debug('periodenddate', periodenddate)
                    log.debug('periodname', periodname)

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

            //return startDate + ',' + endDate + ',' + periodName;
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
            //var usageRemaining = objContext.getRemainingUsage();
            // Valida si es OneWorld
            // var featuresubs = runtime.isFeatureInEffect({
            //     feature: 'SUBSIDIARIES'
            // });
            var _cont = 0;

            // Consulta de Cuentas
            if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {
                /* Latamready - CO Daily book without closing */
                var savedsearch = search.load({
                    type: 'accountingtransaction',
                    id: 'customsearch_lmry_co_diario_scierre'
                });
                //agrega los params como filtros
                var filtros = savedsearch.filters
                filtros.push(
                    search.createFilter({
                        name: 'postingperiod',
                        join: 'transaction',
                        operator: 'is',
                        values: [paramperiodo]
                    })
                );
                filtros.push(
                    search.createFilter({
                        name: 'accountingbook',
                        operator: 'is',
                        values: [paramMultibook]
                    })
                );
                // Valida si es OneWorld
                if (featuresubs) {
                    filtros.push(
                        search.createFilter({
                            name: 'subsidiary',
                            operator: 'is',
                            values: [paramsubsidi]
                        })
                    );
                }
                // var tranIdNumberColumn = search.createColumn({
                //     name: 'formulatext',
                //     formula: 'NVL({transaction.tranid},{transaction.transactionnumber})',
                //     summary: 'GROUP',
                //     label: '8.-Tran Id or transaction number'
                // });
                // savedsearch.columns.push(tranIdNumberColumn);
            } else {
                log.error('normal', "normal");

                var savedsearch = search.load({
                    type: 'transaction',
                    id: 'customsearch_lmry_co_diario_scierre_tran'
                });

                var filtros = savedsearch.filters
                filtros.push(
                    search.createFilter({
                        name: 'postingperiod',
                        operator: 'is',
                        values: [paramperiodo]
                    })
                );
                // Valida si es OneWorld
                if (featuresubs) {
                    filtros.push(
                        search.createFilter({
                            name: 'subsidiary',
                            operator: 'is',
                            values: [paramsubsidi]
                        })
                    )
                }
                // var tranIdNumberColumn = search.createColumn({
                //     name: 'formulatext',
                //     formula: 'NVL({tranid},{transactionnumber})',
                //     summary: 'GROUP',
                //     label: '8.-Tran Id or transaction number'
                // });
                // savedsearch.columns.push(tranIdNumberColumn);
            }

            var searchresult = savedsearch.run();
            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);;
                //log.debug('objResult', objResult)
                if (objResult != null) {

                    if (objResult.length < 1000) {
                        DbolStop = true;
                    }
                    var intLength = objResult.length;
                    for (var i = 0; i < intLength; i++) {
                        //columns = objResult[i].getAllColumns();
                        columns = objResult[i].columns;
                        arrAuxiliar = new Array();
                        //var tranIdCamp = objResult[i].getValue(columns[8]);
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
                        //3. sum debitos
                        if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
                            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                        else
                            arrAuxiliar[3] = 0.00;
                        //4. sum credito
                        if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
                            arrAuxiliar[4] = objResult[i].getValue(columns[4])
                        else
                            arrAuxiliar[4] = 0.00;
                        //5. tipo de cuenta
                        if (objResult[i].getText(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
                            arrAuxiliar[5] = objResult[i].getText(columns[5]);
                        else
                            arrAuxiliar[5] = '';
                        //6. Numero de cuenta
                        if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
                            arrAuxiliar[6] = objResult[i].getValue(columns[6]);
                        else
                            arrAuxiliar[6] = '';
                        //7. 
                        // if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined') {
                        //     if (tranIdCamp != null && tranIdCamp != '- None -' && tranIdCamp != 'NaN' && tranIdCamp != 'undefined') {
                        //         arrAuxiliar[7] = objResult[i].getText(columns[7]) + ' - ' + tranIdCamp;
                        //     } else {
                        //         arrAuxiliar[7] = objResult[i].getText(columns[7]);
                        //     }
                        if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined') {
                            arrAuxiliar[7] = objResult[i].getText(columns[7]);
                        } else
                            arrAuxiliar[7] = '';
                        //guarda el arrAuxiliar en el variable global ArrLibroDiario
                        // arrLibroDiario[_cont] = arrAuxiliar;
                        // _cont++;
                        arrLibroDiario.push(arrAuxiliar);
                    }
                    if (!DbolStop) {
                        intDMinReg = intDMaxReg;
                        intDMaxReg += 1000;
                    }
                } else {
                    DbolStop = true;
                }
            }

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

                // Crea el archivo
                //var File = nlapiCreateFile(NameFile, pTipoArchivo, strName);
                //File.setFolder(FolderId);

                var Filed = file.create({
                    name: NameFile,
                    fileType: pTipoArchivo,
                    contents: strName,
                    folder: FolderId
                });


                // Termina de grabar el archivo
                var idfile = Filed.save();
                //log.debug('Filed.save', idfile);
                // Trae URL de archivo generado
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
                        id: paramidrpt
                    });
                    record.setValue({
                        fieldId: RecordTable[0],
                        value: NameFile
                    });
                    //record.setValue({ fieldId: RecordTable[1], value: periodname });
                    //record.setValue({ fieldId: RecordTable[2], value: companyname });
                    record.setValue({
                        fieldId: RecordTable[3],
                        value: urlfile
                    });
                    //record.setValue({ fieldId: RecordTable[4], value: usuario });
                    //record.setValue({ fieldId: RecordTable[5], value: multibook_name });
                    record.save();
                    // Envia mail de conformidad al usuario
                    //libreria.sendrptuser(namereport, 3, NameFile);
                    libreria.sendrptuser(NameFile);

                    // var record = nlapiLoadRecord(RecordName, paramidrpt); // generator_log
                    // record.setFieldValue(RecordTable[0], NameFile); // name
                    // record.setFieldValue(RecordTable[1], periodname); // postingperiod
                    // record.setFieldValue(RecordTable[2], companyname); // subsidiary
                    // record.setFieldValue(RecordTable[3], urlfile); // url_file
                    // record.setFieldValue(RecordTable[4], usuario); // employee
                    // record.setFieldValue(RecordTable[5], multibook_name); // multi
                    // nlapiSubmitRecord(record, true);
                    // sendrptuser(NameFile);
                }
            } else {
                // Debug
                log.error('Creacion de Excel', 'No se existe el folder');
            }
        }

        function Periodo(periodo) {
            var auxfech = '';

            auxanio = periodo.substr(-4);
            switch (periodo.substring(0, 3).toLowerCase()) {
                case 'ene', 'jan':
                    auxmess = '01';
                    break;
                case 'feb':
                    auxmess = '02';
                    break;
                case 'mar':
                    auxmess = '03';
                    break;
                case 'abr', 'apr':
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
                case 'ago', 'aug':
                    auxmess = '08';
                    break;
                case 'set', 'sep':
                    auxmess = '09';
                    break;
                case 'oct':
                    auxmess = '10';
                    break;
                case 'nov':
                    auxmess = '11';
                    break;
                case 'dic', 'dec':
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

            // Valida si es OneWorld

            var featuresubs = runtime.isFeatureInEffect({
                feature: 'SUBSIDIARIES'
            });

            var savedsearch = search.load({
                type: 'account',
                id: 'customsearch_lmry_account_context'
            });


            var filtros = savedsearch.filters
                // Valida si es OneWorld
            if (featuresubs == true) {
                filtros.push(
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


            var col_search_puc6_den = search.createColumn({
                name: 'formulatext',
                summary: 'group',
                formula: '{custrecord_lmry_co_puc_d6_description}'
            });

            savedsearch.columns = [col_search_puc6_id, col_search_puc6_den];
            // savedsearch.addColumn(col_search_puc6_id);

            // var col_search_puc6_den = new nlobjSearchColumn('formulatext', null, 'group');
            // col_search_puc6_den.setFormula('{custrecord_lmry_co_puc_d6_description}');

            // savedsearch.addColumn(col_search_puc6_den);

            var searchresult = savedsearch.run();



            while (!DbolStop) {

                //var objResult = searchresult.getResults(intDMinReg, intDMaxReg);
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    var intLength = objResult.length;
                    log.debug('intDMinReg, intDMaxReg, length: ', intDMinReg + ',' + intDMaxReg + ',' + intLength);

                    if (intLength == 0) {
                        DbolStop = true;
                    } else {
                        for (var i = 0; i < intLength; i++) {
                            // Cantidad de columnas de la busqueda

                            //columns = objResult[i].getAllColumns();
                            columns = objResult[i].columns;
                            arrAuxiliar = new Array();
                            for (var col = 0; col < columns.length; col++) {
                                if (col == 3) {
                                    arrAuxiliar[col] = objResult[i].getText(columns[col]);
                                } else {
                                    arrAuxiliar[col] = objResult[i].getValue(columns[col]);
                                }
                            }
                            if (arrAuxiliar[3] == multibook_name) {
                                arrAccountingContext[contador_auxiliar] = arrAuxiliar;
                                contador_auxiliar++;
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

        function obtenerCuenta(numero_cuenta) {
            for (var i = 0; i < arrAccountingContext.length; i++) {
                if (numero_cuenta == arrAccountingContext[i][0]) {
                    var number_cta_aux = arrAccountingContext[i][1];
                    for (var j = 0; j < arrAccountingContext.length; j++) {

                        if (number_cta_aux == arrAccountingContext[j][0]) {
                            var arrPuc = new Array();

                            arrPuc[0] = arrAccountingContext[j][4];
                            arrPuc[1] = arrAccountingContext[j][5];

                            return arrPuc;
                        }
                    }
                }
            }
            return numero_cuenta;
        }

        function CambioDeCuentas() {

            if (arrAccountingContext != null && arrAccountingContext != 0) {
                for (var i = 0; i < arrLibroDiario.length; i++) {
                    if (arrLibroDiario[i][5] == 'Bank' || arrLibroDiario[i][5] == 'Accounts Payable' || arrLibroDiario[i][5] == 'Accounts Receivable' ||
                        arrLibroDiario[i][5] == 'Banco' || arrLibroDiario[i][5] == 'Cuentas a pagar' || arrLibroDiario[i][5] == 'Cuentas a cobrar') {
                        var cuenta_act = obtenerCuenta(arrLibroDiario[i][6]);

                        if (cuenta_act != arrLibroDiario[i][6]) {
                            arrLibroDiario[i][1] = cuenta_act[0];
                            arrLibroDiario[i][2] = cuenta_act[1];
                        }
                    }
                }
            }
        }

        function OrdenarCuentas() {
            var arrAux = new Array();
            var j = 0;
            log.debug('arrLibroDiario', arrLibroDiario);
            for (var i = 0; i < arrLibroDiario.length; i++) {
                arrAux[j] = arrLibroDiario[i];

                if (arrLibroDiario[i + 1] != null && arrLibroDiario[i + 1] != 0) {
                    while (arrLibroDiario[i][0] == arrLibroDiario[i + 1][0] && arrLibroDiario[i][1] == arrLibroDiario[i + 1][1]) {
                        arrAux[j][0] = arrLibroDiario[i + 1][0];
                        arrAux[j][1] = arrLibroDiario[i + 1][1];
                        arrAux[j][2] = arrLibroDiario[i + 1][2];
                        arrAux[j][3] = Number(arrAux[j][3]) + Number(arrLibroDiario[i + 1][3]);
                        arrAux[j][4] = Number(arrAux[j][4]) + Number(arrLibroDiario[i + 1][4]);
                        arrAux[j][5] = arrLibroDiario[i + 1][5];
                        arrAux[j][6] = arrLibroDiario[i + 1][6];
                        arrAux[j][7] = arrLibroDiario[i + 1][7];

                        i++;
                        if (i == arrLibroDiario.length - 1) {
                            break;
                        }
                    }
                    j++;
                }
            }
            return arrAux;
        }

        function ObtenerParametrosYFeatures() {
            paramsubsidi = objContext.getParameter({
                name: 'custscript_lmry_co_librod_subsi_sc'
            });
            paramperiodo = objContext.getParameter({
                name: 'custscript_lmry_co_librod_period_sc'
            });
            paramidrpt = objContext.getParameter({
                name: 'custscript_lmry_co_librod_idreport_sc'
            });
            paramMultibook = objContext.getParameter({
                name: 'custscript_lmry_co_librod_multibook_sc'
            });
            paramEndPeriod = objContext.getParameter({
                name: 'custscript_lmry_co_adjust_diario_sc'
            });
            log.debug('Parametros', paramsubsidi + ', ' + paramperiodo + ', ' + paramidrpt + ', ' + paramMultibook + ', ' + paramEndPeriod);

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
                //datos de la empresa
                companyName = configpage.getValue('companyname');
                companyruc = configpage.getValue('employerid');
            }
            companyruc = companyruc.replace(' ', '');
            log.debug('companyruc', companyruc);
        }

        function getGlobalLabels() {
            var labels = {
                "Alert1": {
                    "es": "LIBRO DIARIO SIN CIERRE",
                    "en": "DIARY BOOK WITHOUT CLOSURE",
                    "pt": "LIVRO DE DIARIO SEM FECHAMENTO"
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
            execute: execute
        };

    });