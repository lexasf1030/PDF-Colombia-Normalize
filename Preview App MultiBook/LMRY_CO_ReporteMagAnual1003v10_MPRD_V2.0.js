/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ReporteMagAnual1003v10_MPRD_V2.0.js      ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Jul 02 2020   Edwin        Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search', 'N/log', 'require', 'N/file', "N/config", 'N/runtime', 'N/query', "N/format", "N/record", "N/task", 'N/encode', "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js",
        "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"
    ],

    function(search, log, require, fileModulo, config, runtime, query, format, recordModulo, task, encode, libreria, libFeature) {
        /**
         * Input Data for processing
         *
         * @return Array,Object,Search,File
         *
         * @since 2016.1
         */
        var objContext = runtime.getCurrentScript();
        // Nombre del Reporte
        var namereport = "CO - Form 1003 Retencion Practicada Anual";
        var LMRY_script = 'LMRY_CO_ReporteMagAnual1003v10_MPRD_V2.0.js';

        //Parametros
        var paramSubsidiaria = objContext.getParameter({
            name: 'custscript_lmry_co_rpt_1003_subsidiaria'
        });
        var paramPeriodo = objContext.getParameter({
            name: 'custscript_lmry_co_rpt_1003_periodo'
        });
        var paramMultibook = objContext.getParameter({
            name: 'custscript_lmry_co_rpt_1003_multibook'
        });
        var paramIDReport = objContext.getParameter({
            name: 'custscript_lmry_co_rpt_1003_feature'
        });
        var paramIDRecord = objContext.getParameter({
            name: 'custscript_lmry_co_rpt_1003_recordid'
        });
        var paramByVersion = objContext.getParameter({
            name: 'custscript_lmry_co_rpt_1003_by_version'
        });
        var paramConcepto = objContext.getParameter({
            name: 'custscript_lmry_co_rpt_1003_concepto'
        });

        var file_size = 7340032;

        //
        periodStartDate = '';
        periodEndDate = '';

        //Features
        //etos son para otra cosa xD
        var feature_Project = runtime.isFeatureInEffect({
            feature: "JOBS"
        });

        var feature_magProject = runtime.isFeatureInEffect({
            feature: "ADVANCEDJOBS"
        });
        var featureSubsidiaria = runtime.isFeatureInEffect({
            feature: "SUBSIDIARIES"
        });
        var featureMultibook = runtime.isFeatureInEffect({
            feature: "MULTIBOOK"
        });

        var featureCabecera = null;
        var featureLineas = null;


        //Datos de Subsidiaria
        var companyName = null;
        var companyRuc = null;

        //Period enddate falta implemnatr
        var periodMonth = null;
        var periodStart = null;
        var periodName = null;

        //Nombre de libro contable
        var multibookName = '';

        //Array Temporal
        var arrayCodsT = [];
        var arrayCodsR = [];
        var arrayCodsRJ = [];
        var arrayCodsSR = [];

        var language = runtime.getCurrentScript().getParameter({
            name: 'LANGUAGE'
        }).substring(0, 2);

        var GLOBAL_LABELS = getGlobalLabels();

        /**
         * Input Data for processing
         *
         * @return Array,Object,Search,File
         *
         * @since 2016.1
         */

        function getInputData() {
            try {
                log.debug('aqui empieza el script');
                //esta parte es para obtener valores que vo a necesitar :v

                ObtenerParametrosYFeatures();
                log.debug('haber los feature', featureCabecera + '....' + featureLineas);

                var arrTransacciones = ObtenerTransaccionesCMI();
                log.debug('transacciones CMI', arrTransacciones);
                var arrReteJournals = obtenerReteJournals();
                log.debug('transacciones ReteJournals', arrReteJournals);
                arrTransacciones = arrTransacciones.concat(arrReteJournals);
                var arrJournalsAMano = obtenerJournalsAMano();
                log.debug('transacciones JournalsAMano', arrJournalsAMano);
                arrTransacciones = arrTransacciones.concat(arrJournalsAMano);
                log.debug('transacciones Totales', arrTransacciones);
                log.debug('cantidad de transacciones', arrTransacciones.length);


                return arrTransacciones;

            } catch (error) {
                log.error('Error de getInputData', error);
                libreria.sendMail('LMRY_script', ' [ getInputData ] ' + error);
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
                var arrTemp = JSON.parse(context.value);
                ObtenerParametrosYFeatures();

                //busqueda de valores de customer de la transaccion
                ArrCustomer = ObtenerDatosCustomer(arrTemp[1]);
                //log.error('Arreglo de customer',ArrCustomer);

                //esta parte solo es para la busqueda de Journals
                if (arrTemp.length == 11) {
                    log.debug('mousecaherramienta misteriosa 8', arrTemp);
                    if (arrTemp[6] != '' && arrTemp != null && arrTemp[8] == '1') {
                        if (featureMultibook) {
                            var datos_r = ObtenerDatosTransaccion(arrTemp[4], 2);
                            log.debug('datos_r', datos_r);
                            if (arrTemp[0] != 'CustInvc') {
                                datos_r[0] = datos_r[0] * -1;
                                datos_r[1] = datos_r[1] * -1;
                            }
                        } else {
                            var datos_r = ObtenerDatosTransaccion(arrTemp[4], 1);
                            log.debug('datos_r', datos_r);
                            if (arrTemp[0] != 'CustInvc') {
                                datos_r[1] = datos_r[1] * -1;
                            }
                        }

                        if (featureMultibook) {
                            var verdaderoCod = VerdaderoCodigo(arrTemp[7]);
                            arrTemp[5] = verdaderoCod[0];
                            var id_formatc = verdaderoCod[1];

                        } else {
                            var verdaderoCod = VerdaderoCodigo(arrTemp[7]);
                            arrTemp[5] = verdaderoCod[0];
                            var id_formatc = verdaderoCod[1];
                        }

                        log.debug('concepto', arrTemp[5] + '-.-.' + datos_r[0] + '----' + datos_r[1]);
                        if (id_formatc == 2) {
                            context.write({
                                key: arrTemp[1] + '|' + arrTemp[5],
                                value: {
                                    Concepto: arrTemp[5],
                                    Customer: ArrCustomer,
                                    Monto_Base: datos_r[0],
                                    Retencion: datos_r[1],
                                    Transaccion: arrTemp[4]
                                }
                            });
                        }
                        return true;
                    } else {
                        var estadotransac = arrTemp[2].split('Reclasification').length > 1;
                        //var estadotransac=false;
                        log.debug('el estado transac es: ', estadotransac);
                        if (estadotransac == true) {
                            if (arrayCodsRJ.indexOf(arrTemp[9]) == -1) {
                                log.debug('entro al map reclas', estadotransac);
                                arrayCodsRJ.push(arrTemp[9]);
                                var datos_r = ObtenerDatosTaxResultRJL(arrTemp[4], arrTemp[9], 2);
                                for (key in datos_r) {
                                    var monto_base = 0;
                                    var monto_total = 0;
                                    var datos_temp = datos_r[key].split('|');

                                    monto_base = round(datos_temp[2]);
                                    monto_total = round(datos_temp[3]);

                                    log.debug('monto base rete test dentre del else', monto_base + '--' + monto_total);

                                    if (arrTemp[0] != 'CustInvc') {
                                        monto_base = monto_base * -1;
                                        monto_total = monto_total * -1;
                                    }


                                    if (featureMultibook) {
                                        var verdaderoCod = VerdaderoCodigoRJL(datos_temp[0], datos_temp[1]);
                                        arrTemp[5] = verdaderoCod[0];
                                        var id_formatc = verdaderoCod[1];

                                    } else {
                                        var verdaderoCod = VerdaderoCodigoRJL(datos_temp[0], datos_temp[1]);
                                        arrTemp[5] = verdaderoCod[0];
                                        var id_formatc = verdaderoCod[1];
                                    }

                                    log.debug('codigo_concepto', arrTemp[5] + '-.-.' + monto_base + '---' + monto_total);
                                    if (id_formatc == 2) {
                                        context.write({
                                            key: arrTemp[1] + '|' + arrTemp[5],
                                            value: {
                                                Concepto: arrTemp[5],
                                                Customer: ArrCustomer,
                                                Monto_Base: monto_base,
                                                Retencion: monto_total,
                                                Transaccion: arrTemp[4]

                                            }
                                        });
                                    }
                                }
                            }
                        } else {
                            log.debug('map sin reclas');
                            if (arrayCodsT.indexOf(arrTemp[4]) == -1) {
                                log.debug('entro al map sin reclas dentro de validacion', arrayCodsT);
                                arrayCodsT.push(arrTemp[4]);
                                log.debug('array cods luego depush', arrayCodsT);
                                var datos_r = ObtenerDatosTaxResultRJL(arrTemp[4], arrTemp[9], 1);
                                for (key in datos_r) {
                                    var monto_base = 0;
                                    var monto_total = 0;
                                    var datos_temp = datos_r[key].split('|');

                                    monto_base = round(datos_temp[2]);
                                    monto_total = round(datos_temp[3]);

                                    log.debug('monto base rete test dentre del else', monto_base + '--' + monto_total);

                                    if (arrTemp[0] != 'CustInvc') {
                                        monto_base = monto_base * -1;
                                        monto_total = monto_total * -1;
                                    }


                                    if (featureMultibook) {
                                        var verdaderoCod = VerdaderoCodigoRJL(datos_temp[0], datos_temp[1]);
                                        arrTemp[5] = verdaderoCod[0];
                                        var id_formatc = verdaderoCod[1];
                                    } else {
                                        var verdaderoCod = VerdaderoCodigoRJL(datos_temp[0], datos_temp[1]);
                                        arrTemp[5] = verdaderoCod[0];
                                        var id_formatc = verdaderoCod[1];
                                    }

                                    log.debug('codigo_concepto', arrTemp[5] + '-.-.' + monto_base + '---' + monto_total);
                                    if (id_formatc == 2) {
                                        context.write({
                                            key: arrTemp[1] + '|' + arrTemp[5],
                                            value: {
                                                Concepto: arrTemp[5],
                                                Customer: ArrCustomer,
                                                Monto_Base: monto_base,
                                                Retencion: monto_total,
                                                Transaccion: arrTemp[4]

                                            }
                                        });
                                    }
                                }
                            }
                        }

                        return true;
                    }
                }
                //esta parte  es para journals hcechos a mano
                if (arrTemp.length == 5) {
                    log.debug('mousecaherramienta misteriosa 5', arrTemp);
                    //0. internal id journal
                    //1. id customer
                    //2. es linea de debito o credito
                    //3. cuenta no multibook
                    //4. line unique key
                    log.debug('journals hechos a mano');
                    if (featureMultibook) {
                        var verdaderoCod = VerdaderoCodigo(arrTemp[3]);
                        arrTemp[3] = verdaderoCod[0];
                        var id_formatc = verdaderoCod[1];
                        log.debug('entro al if multi a mano', arrTemp[3]);
                    } else {
                        var verdaderoCod = VerdaderoCodigo(arrTemp[3]);
                        arrTemp[3] = verdaderoCod[0];
                        var id_formatc = verdaderoCod[1];
                        log.debug('entro al else sin multi a mano', arrTemp[3]);
                    }
                    montos = ObtenerMontosTaxResult(arrTemp[0], arrTemp[4]);

                    log.debug("concepto en journal a mano", arrTemp[3] + '|' + montos[0] + '--' + montos[1]);
                    if (montos.length != 0 && id_formatc == 2) {
                        montos[0] = round(montos[0]);
                        montos[1] = round(montos[1]);
                        context.write({
                            key: arrTemp[1] + '|' + arrTemp[3],
                            value: {
                                Concepto: arrTemp[3],
                                Customer: ArrCustomer,
                                Monto_Base: montos[0],
                                Retencion: montos[1],
                                Transaccion: arrTemp[0]
                            }
                        });
                    }
                    return true;
                }
                if (arrTemp[6] != 'SI') {
                    // se verifica el monto de acabcera de retencion
                    log.debug('mousecaherramienta misteriosa !SI', arrTemp);
                    if (featureMultibook) {
                        var valores = ObtenerDatosTransaccion(arrTemp[3], 2);
                        if (arrTemp[7] == 'CustCred') {
                            valores[0] = valores[0] * -1;
                            valores[1] = valores[1] * -1;
                        }
                    } else {
                        var valores = ObtenerDatosTransaccion(arrTemp[3], 1);
                        if (arrTemp[7] == 'CustCred') {
                            valores[1] = valores[1] * -1;
                        }
                    }

                    var verdaderoCod = VerdaderoCodigo(arrTemp[5]);
                    codigo_concepto = verdaderoCod[0];
                    var id_formatc = verdaderoCod[1];
                    log.debug('los valores son: ', codigo_concepto + '.-.' + valores[0] + '----' + valores[1]);
                    //log.error('mira el codigo', codigo_concepto + 'Mnt Base: '+ valores[0] + 'mnt ret:' + valores[1]);
                    if (id_formatc == 2) {
                        context.write({
                            key: arrTemp[1] + '|' + codigo_concepto,
                            value: {
                                Concepto: codigo_concepto,
                                Customer: ArrCustomer,
                                Monto_Base: valores[0],
                                Retencion: valores[1],
                                Transaccion: arrTemp[3]
                            }
                        });
                    }
                    return true;


                } else {
                    //esta parte es para lineas
                    log.debug('mousecaherramienta misteriosa ELSE', arrTemp);

                    var estadotransac = arrTemp[8].split('Reclasification').length > 1;
                    if (estadotransac == true) {
                        if (arrayCodsR.indexOf(arrTemp[9]) == -1) {
                            log.debug('entro al map reclas ICM', estadotransac);
                            arrayCodsR.push(arrTemp[9]);
                            var datos_r = ObtenerDatosTaxResultRJL(arrTemp[3], arrTemp[9], 2);
                            for (key in datos_r) {
                                var monto_base = 0;
                                var monto_total = 0;
                                var datos_temp = datos_r[key].split('|');

                                monto_base = round(datos_temp[2]);
                                monto_total = round(datos_temp[3]);

                                log.debug('monto base rete test dentre del else', monto_base + '--' + monto_total);

                                if (arrTemp[7] == 'CustCred') {
                                    monto_base = monto_base * -1;
                                    monto_total = monto_total * -1;
                                }

                                if (featureMultibook) {
                                    var verdaderoCod = VerdaderoCodigo(arrTemp[5]);
                                    codigo_concepto = verdaderoCod[0];
                                    var id_formatc = verdaderoCod[1];

                                } else {
                                    var verdaderoCod = VerdaderoCodigo(arrTemp[5]);
                                    codigo_concepto = verdaderoCod[0];
                                    var id_formatc = verdaderoCod[1];
                                }

                                log.debug('mira el codigo', codigo_concepto + '---' + monto_base + '---' + monto_total);
                                if (id_formatc == 2) {
                                    context.write({
                                        key: arrTemp[1] + '|' + codigo_concepto,
                                        value: {
                                            Concepto: codigo_concepto,
                                            Customer: ArrCustomer,
                                            Monto_Base: monto_base,
                                            Retencion: monto_total,
                                            Transaccion: arrTemp[3]
                                        }
                                    });
                                }
                            }
                        }
                    } else {

                        if (arrayCodsSR.indexOf(arrTemp[3]) == -1) {
                            log.debug('entro al map sin reclas dentro de validacion CMI', arrayCodsSR);
                            arrayCodsSR.push(arrTemp[3]);
                            log.debug('array cods luego depush', arrayCodsSR);
                            var datos_r = ObtenerDatosTaxResultRJL(arrTemp[3], arrTemp[9], 1);
                            for (key in datos_r) {
                                var monto_base = 0;
                                var monto_total = 0;
                                var datos_temp = datos_r[key].split('|');

                                monto_base = round(datos_temp[2]);
                                monto_total = round(datos_temp[3]);

                                log.debug('monto base rete test dentre del else', monto_base + '--' + monto_total);

                                if (arrTemp[7] != 'CustInvc') {
                                    monto_base = monto_base * -1;
                                    monto_total = monto_total * -1;
                                }


                                if (featureMultibook) {
                                    var verdaderoCod = VerdaderoCodigo(arrTemp[5]);
                                    codigo_concepto = verdaderoCod[0];
                                    var id_formatc = verdaderoCod[1];

                                } else {
                                    var verdaderoCod = VerdaderoCodigo(arrTemp[5]);
                                    codigo_concepto = verdaderoCod[0];
                                    var id_formatc = verdaderoCod[1];
                                }

                                log.debug('codigo_concepto', codigo_concepto + '-.-.' + monto_base + '---' + monto_total);
                                if (id_formatc == 2) {
                                    context.write({
                                        key: arrTemp[1] + '|' + codigo_concepto,
                                        value: {
                                            Concepto: codigo_concepto,
                                            Customer: ArrCustomer,
                                            Monto_Base: monto_base,
                                            Retencion: monto_total,
                                            Transaccion: arrTemp[3]
                                        }
                                    });
                                }
                            }
                        }
                    }
                    return true;
                }

            } catch (err) {
                log.error('Error de MAP', error);
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
            log.debug('llego al reduce', context.key);
            var resultArray = context.values;
            var ArrCustomer = [];
            var MntBase = 0;
            var MntRetencion = 0;
            var Concepto = 0;
            var M_base = {};
            var objResult, amount = 0;
            for (var i = 0; i < resultArray.length; i++) {
                objResult = JSON.parse(resultArray[i]);
                ArrCustomer = objResult.Customer;
                Concepto = objResult.Concepto;
                log.debug('ID transaccion:monto', 'ID: ' + objResult.Transaccion + ' Mnt Retenci: ' + objResult.Retencion);
                MntRetencion += Number(objResult.Retencion);
                MntBase += Number(objResult.Monto_Base);
                /*if (objResult.ID_transaction != null && objResult.ID_transaction != undefined) {
                  if (!M_base[objResult.ID_transaction]) {
                     M_base[objResult.ID_transaction] = {
                       "monto" : objResult.Monto_Base
                     }
                  }
                }else {
        
                }*/

            }

            for (var k in M_base) {
                MntBase += M_base[k].monto;
            }

            log.debug('concepto', Concepto);
            log.debug('haber', ArrCustomer);
            log.debug('monto base', MntBase);
            log.debug('MntRetencion', MntRetencion);
            mntbase = MntBase.toString();
            if (mntbase != '0') {

                var ArrLineas = new Array();
                ArrLineas[0] = Concepto;
                // tipo documento
                ArrLineas[1] = ArrCustomer[0];
                //digito verificador
                ArrLineas[2] = ArrCustomer[1];
                //appelidos
                ArrLineas[3] = ArrCustomer[2];
                //nombres
                ArrLineas[4] = ArrCustomer[3];
                // razon social
                ArrLineas[5] = ArrCustomer[4];
                // departamento
                ArrLineas[6] = ArrCustomer[5];
                // municipio
                ArrLineas[7] = ArrCustomer[6];
                //direccion
                ArrLineas[8] = ArrCustomer[7];
                //numero de identificacion
                ArrLineas[9] = ArrCustomer[8];

                ArrLineas[10] = MntBase;
                ArrLineas[11] = MntRetencion;


                context.write({
                    key: context.key,
                    value: {
                        Arreglo: ArrLineas
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
                ObtenerParametrosYFeatures();
                ObtenerDatosSubsidiaria();
                arrValores = [];

                context.output.iterator().each(function(key, value) {

                    var obj = JSON.parse(value);
                    arrValores.push(obj.Arreglo);

                    return true;
                });
                //log.debug('Todo lo que veras',vector_percepciones);
                log.debug('ya todo junto', arrValores);
                if (arrValores.length == 0) {
                    NoData();
                    return true;
                }
                Sumatotal = ObtenerValorTotal(arrValores);
                numeroEnvio = obtenerNumeroEnvio();
                GenerarExcel(Sumatotal, numeroEnvio, arrValores);
                GenerarXml(Sumatotal, numeroEnvio, arrValores);

            } catch (error) {
                log.error('Error de SUMMARIZE', error);

                //libreria.sendemailTranslate(' [ summarize ] ' + error);
            }
        }

        function obtenerNumeroEnvio() {
            var numeroLote = 1;
            var savedSearch = search.create({
                type: 'customrecord_lmry_co_lote_rpt_magnetic',
                filters: [
                    search.createFilter({
                        name: 'internalid',
                        join: 'custrecord_lmry_co_id_magnetic_rpt',
                        operator: search.Operator.IS,
                        values: [paramByVersion]
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
                //            if (loteXRptMgnResult.numeroLote == null) {
                var loteXRptMgnRecord = recordModulo.create({
                    type: 'customrecord_lmry_co_lote_rpt_magnetic'
                });

                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_id_magnetic_rpt',
                    value: paramByVersion
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
                });

                loteXRptMgnRecord.save();

            } else {
                var columns = objResult[0].columns;
                var internalId = objResult[0].getValue(columns[0]);
                numeroLote = Number(objResult[0].getValue(columns[1])) + 1;
                var loteXRptMgnRecord = recordModulo.load({
                    type: 'customrecord_lmry_co_lote_rpt_magnetic',
                    id: internalId
                });

                //log.error('mira',loteXRptMgnRecord);
                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_lote',
                    value: numeroLote
                });

                loteXRptMgnRecord.save();
            }

            return numeroLote;
        }

        function ObtenerMontosTaxResult(id_transatcion, id_line) {

            var search_journaltax = search.create({
                type: "customrecord_lmry_br_transaction",
                filters: [
                    ["custrecord_lmry_br_transaction", "anyof", id_transatcion],
                    "AND", ["custrecord_lmry_lineuniquekey", "equalto", id_line]
                ],
                columns: [
                    search.createColumn({
                        name: "custrecord_lmry_base_amount",
                        label: "Latam - Base Amount"
                    }),
                    search.createColumn({
                        name: "custrecord_lmry_br_total",
                        label: "Latam - Total"
                    }),
                    search.createColumn({
                        name: "custrecord_lmry_accounting_books",
                        label: "Latam - Accounting Books"
                    }),
                    search.createColumn({
                        name: "formulanumeric",
                        formula: "NVL({custrecord_lmry_base_amount_local_currc},0)",
                        label: "Formula (Numeric)"
                    }),
                    search.createColumn({
                        name: "formulanumeric",
                        formula: "NVL({custrecord_lmry_amount_local_currency},0)",
                        label: "Formula (Numeric)"
                    })
                ]
            });

            var arrReturn = [];
            var resultado = search_journaltax.run().getRange(0, 100);
            if (resultado.length != 0) {
                var columns = resultado[0].columns;

                if (resultado[0].getValue(columns[2]) != '') {
                    var multi = exchange_rate(resultado[0].getValue(columns[2]));
                } else {
                    var multi = 1;
                }
                arrReturn[0] = resultado[0].getValue(columns[0]) * multi;
                arrReturn[1] = resultado[0].getValue(columns[1]) * multi;
                var auxColum1 = resultado[0].getValue(columns[3]);
                var auxColum2 = resultado[0].getValue(columns[4]);
                if (Number(auxColum1) > 0) {
                    arrReturn[0] = auxColum1;
                    arrReturn[1] = auxColum2;
                }

            }
            return arrReturn;
        }

        function ObtenerItems(id_transatcion) {
            var ArrItem = [];
            var search_item = search.create({
                type: "transaction",
                filters: [
                    ["posting", "is", "T"],
                    "AND", ["voided", "is", "F"],
                    "AND", ["taxline", "is", "F"],
                    "AND", ["memorized", "is", "F"],
                    "AND", ["internalid", "anyof", id_transatcion],
                    "AND", ["mainline", "is", "F"],
                    "AND", ["formulatext: CASE WHEN {taxitem} = 'UNDEF-CO' OR {taxitem} = 'UNDEF CO' THEN 1 ELSE 0 END", "is", "0"],
                    "AND", ["cogs", "is", "F"]
                ],
                columns: [
                    search.createColumn({ name: "item", summary: "GROUP", label: "Item" }),
                    search.createColumn({
                        name: "internalid",
                        join: "item",
                        summary: "GROUP",
                        label: "Internal ID"
                    })
                ]
            });
            var resultados = search_item.run().getRange(0, 100);
            for (var i = 0; i < resultados.length; i++) {
                var columns = resultados[i].columns;
                ArrItem.push(resultados[i].getValue(columns[1]));
            }
            return ArrItem;
        }

        function ObtenerDatosTransaccion(id_transaction, type) {
            if (type == 1) {
                var transactionSearchObj = search.create({
                    type: "transaction",
                    filters: [
                        ["type", "anyof", "CustCred", "CustInvc"],
                        "AND", ["internalid", "anyof", id_transaction],
                        "AND", ["mainline", "is", "T"],
                    ],
                    settings: [
                        search.createSetting({
                            name: 'consolidationtype',
                            value: 'NONE'
                        })
                    ],
                    columns: [
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custbody_lmry_co_retefte.custrecord_lmry_wht_salebase.id}",
                            label: "0. base id"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{taxtotal}",
                            label: "1 tax total"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{totalamount}",
                            label: "2. gross total"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{totalamount} - {taxtotal}",
                            label: "3. net total"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custbody_lmry_co_retefte_amount}",
                            label: "4 monto de retencion"
                        }),
                        search.createColumn({ name: "exchangerate", label: "5 Exchange Rate" })
                    ]
                });

                if (featureMultibook) {
                    var multibookFilter = search.createFilter({
                        name: 'accountingbook',
                        join: 'accountingtransaction',
                        operator: search.Operator.IS,
                        values: [paramMultibook]
                    });
                    transactionSearchObj.filters.push(multibookFilter);

                    //6. miralo
                    var colmuna_rate = search.createColumn({
                        name: "exchangerate",
                        join: "accountingTransaction",
                        label: "Exchange Rate"
                    });
                    transactionSearchObj.columns.push(colmuna_rate);
                }

                var objResult = transactionSearchObj.run().getRange(0, 20);
                var auxArray = new Array();
                var columns = objResult[0].columns;

                // monto base de la retencion
                if (objResult[0].getValue(columns[0]) == 1) {
                    if (featureMultibook) {
                        auxArray[0] = objResult[0].getValue(columns[2]) * objResult[0].getValue(columns[6]) / objResult[0].getValue(columns[5]);
                    } else {
                        auxArray[0] = objResult[0].getValue(columns[2]);
                    }
                } else if (objResult[0].getValue(columns[0]) == 2) {
                    if (featureMultibook) {
                        auxArray[0] = objResult[0].getValue(columns[3]) * objResult[0].getValue(columns[6]) / objResult[0].getValue(columns[5]);
                    } else {
                        auxArray[0] = objResult[0].getValue(columns[3]);
                    }
                } else if (objResult[0].getValue(columns[0]) == 3) {
                    if (featureMultibook) {
                        auxArray[0] = objResult[0].getValue(columns[1]) * objResult[0].getValue(columns[6]) / objResult[0].getValue(columns[5]);
                    } else {
                        auxArray[0] = objResult[0].getValue(columns[1]);
                    }
                } else {
                    if (featureMultibook) {
                        auxArray[0] = objResult[0].getValue(columns[3]) * objResult[0].getValue(columns[6]) / objResult[0].getValue(columns[5]);
                    } else {
                        auxArray[0] = objResult[0].getValue(columns[3]);
                    }
                }
                //monto de retencion
                auxArray[1] = objResult[0].getValue(columns[4]);
            } else {
                var transactionSearchObj = search.create({
                    type: "transaction",
                    filters: [
                        ["type", "anyof", "CustCred", "CustInvc"],
                        "AND", ["internalid", "anyof", id_transaction],
                        "AND", ["mainline", "is", "F"],
                        "AND", ["cogs", "is", "F"]
                    ],
                    settings: [
                        search.createSetting({
                            name: 'consolidationtype',
                            value: 'NONE'
                        })
                    ],
                    columns: [
                        search.createColumn({
                            name: "formulanumeric",
                            summary: "GROUP",
                            formula: "{custbody_lmry_co_retefte.custrecord_lmry_wht_salebase.id}",
                            label: "ID SaleBase"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            summary: "SUM",
                            formula: "CASE WHEN {taxline} = 'true' or {taxline} = 'T' THEN 0 ELSE NVL({accountingtransaction.debitamount},0)-NVL({accountingtransaction.creditamount},0) END",
                            label: "Subtotal"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            summary: "SUM",
                            formula: "CASE WHEN {taxline} = 'true' or {taxline} = 'T' THEN NVL({accountingtransaction.debitamount},0)-NVL({accountingtransaction.creditamount},0) ELSE 0 END",
                            label: "Tax Total"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            summary: "GROUP",
                            formula: "{custbody_lmry_co_retefte_amount}",
                            label: "Retencion"
                        })
                    ]
                });

                if (featureMultibook) {
                    var multibookFilter = search.createFilter({
                        name: 'accountingbook',
                        join: 'accountingtransaction',
                        operator: search.Operator.IS,
                        values: [paramMultibook]
                    });
                    transactionSearchObj.filters.push(multibookFilter);
                }

                var objResult = transactionSearchObj.run().getRange(0, 20);
                var auxArray = new Array();
                var columns = objResult[0].columns;

                // monto base de la retencion
                if (objResult[0].getValue(columns[0]) == 1) {
                    auxArray[0] = Number(Math.abs(objResult[0].getValue(columns[1]))) + Number(Math.abs(objResult[0].getValue(columns[2])));
                } else if (objResult[0].getValue(columns[0]) == 2) {
                    auxArray[0] = Math.abs(objResult[0].getValue(columns[1]));
                } else if (objResult[0].getValue(columns[0]) == 3) {
                    auxArray[0] = Math.abs(objResult[0].getValue(columns[2]));
                } else {
                    auxArray[0] = Math.abs(objResult[0].getValue(columns[1]));
                }
                //monto de retencion
                auxArray[1] = Math.abs(objResult[0].getValue(columns[3]));
            }

            return auxArray;
        }

        function round(number) {
            return Math.round(Number(number) * 100) / 100;
        }

        function exchange_rate(exchangerate) {
            var auxiliar = ('' + exchangerate).split('&');
            var final = '';

            if (featureMultibook) {
                var id_libro = auxiliar[0].split('|');
                var exchange_rate = auxiliar[1].split('|');

                for (var i = 0; i < id_libro.length; i++) {
                    if (Number(id_libro[i]) == Number(paramMultibook)) {
                        final = exchange_rate[i];
                        break;
                    } else {
                        final = exchange_rate[0];
                    }
                }
            } else {
                final = auxiliar[1];
            }
            return final;
        }

        function ObtenerValorTotal(arreglo) {
            var S_total = 0;
            for (var i = 0; i < arreglo.length; i++) {
                S_total += arreglo[i][10];
            }
            return S_total;
        }

        function GenerarXml(Sumatotal, numeroEnvio, arreglo) {
            strXmlVentasXPagar = '';
            ObtenerParametrosYFeatures();
            ObtenerDatosSubsidiaria();
            var today = new Date();
            var anio = today.getFullYear();
            var mes = completar_cero(2, today.getMonth());
            var day = completar_cero(2, today.getDay());
            var hour = completar_cero(2, today.getHours());
            var min = completar_cero(2, today.getMinutes());
            var sec = completar_cero(2, today.getSeconds());
            today = anio + '-' + mes + '-' + day + 'T' + hour + ':' + min + ':' + sec;

            strXmlVentasXPagar += '<?xml version="1.0" encoding="ISO-8859-1"?> \r\n';
            strXmlVentasXPagar += '<mas xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> \r\n';
            strXmlVentasXPagar += '<Cab> \r\n';
            strXmlVentasXPagar += '<Ano>' + paramPeriodo + '</Ano> \r\n';
            strXmlVentasXPagar += '<CodCpt>' + paramConcepto + '</CodCpt> \r\n';
            strXmlVentasXPagar += '<Formato>1003</Formato> \r\n';
            strXmlVentasXPagar += '<Version>10</Version> \r\n';
            strXmlVentasXPagar += '<NumEnvio>' + numeroEnvio + '</NumEnvio> \r\n';
            strXmlVentasXPagar += '<FecEnvio>' + today + '</FecEnvio> \r\n';
            strXmlVentasXPagar += '<FecInicial>' + paramPeriodo + '-01-01</FecInicial> \r\n';
            strXmlVentasXPagar += '<FecFinal>' + paramPeriodo + '-12-31</FecFinal> \r\n';
            strXmlVentasXPagar += '<ValorTotal>' + Sumatotal + '</ValorTotal> \r\n';
            strXmlVentasXPagar += '<CantReg>' + arreglo.length + '</CantReg> \r\n';
            strXmlVentasXPagar += '</Cab>\r\n';

            for (var i = 0; i < arreglo.length; i++) {

                if (Math.abs(Number(arreglo[i][10]).toFixed(0)) > 0 || Math.abs(Number(arreglo[i][11]).toFixed(0)) > 0) {

                    strXmlVentasXPagar += '<rets cpt="' + arreglo[i][0] + '" tdoc="' + arreglo[i][1] + '" nid="' + arreglo[i][9];

                    if (arreglo[i][5] != '' && arreglo[i][5] != null && arreglo[i][5] != '- None -') {

                        if (arreglo[i][2] != '' && arreglo[i][2] != null && arreglo[i][2] != '- None -') {
                            strXmlVentasXPagar += '" dv="' + arreglo[i][2];
                        }
                        strXmlVentasXPagar += '" raz="' + arreglo[i][5];

                    } else {

                        if (arreglo[i][3] && arreglo[i][3].split(' ')[0]) {
                            strXmlVentasXPagar += '" apl1="' + arreglo[i][3].split(' ')[0].replace(/&/g, '&amp;');
                        }
                        if (arreglo[i][3] && arreglo[i][3].split(' ')[1]) {
                            strXmlVentasXPagar += '" apl2="' + arreglo[i][3].split(' ')[1].replace(/&/g, '&amp;');
                        }
                        if (arreglo[i][4] && arreglo[i][4].split(' ')[0]) {
                            strXmlVentasXPagar += '" nomb1="' + arreglo[i][4].split(' ')[0].replace(/&/g, '&amp;');
                        }
                        if (arreglo[i][4] && arreglo[i][4].split(' ')[1]) {
                            strXmlVentasXPagar += '" nomb2="' + arreglo[i][4].split(' ')[1].replace(/&/g, '&amp;');
                        }

                    }
                    strXmlVentasXPagar += '" dir="' + arreglo[i][8] + '" dpto="' + arreglo[i][6] + '" mcpo="' + arreglo[i][7];
                    strXmlVentasXPagar += '" valor="' + Math.abs(Number(arreglo[i][11]).toFixed(0)) + '" ret="' + Math.abs(Number(arreglo[i][10]).toFixed(0));
                    strXmlVentasXPagar += '"/> \r\n';

                }
            }
            strXmlVentasXPagar += '</mas> \r\n';


            SaveFile(strXmlVentasXPagar, '.xml', numeroEnvio);
        }

        function GenerarExcel(Sumatotal, NumEnvio, arreglo) {

            ObtenerParametrosYFeatures();
            ObtenerDatosSubsidiaria();

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

            //Cabecera
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["titulo"][language] + ' </Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["razSocial"][language] + ':' + companyName + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["taxNumber"][language] + ':' + companyRuc + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["period"][language] + ': 01/01/' + paramPeriodo + GLOBAL_LABELS["al"][language] + ' 31/12/' + paramPeriodo + '</Data></Cell>';
            xlsString += '</Row>';
            if (featureMultibook) {
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["multib"][language] + ': ' + multibookName + '</Data></Cell>';
                xlsString += '</Row>';
            }

            //PDF Normalization
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["origin"][language] + ': Netsuite' + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["date"][language] + ': ' + todays + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["time"][language] + ': '+ currentTime + '</Data></Cell>';
            xlsString += '</Row>';

            //End PDF Normalization


            xlsString += '<Row></Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["cpt"][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> TDOC </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> NIT </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> D.V. </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["primerApe"][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["segundApe"][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["primerNom"][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["segundNom"][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["razSocial"][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["direc"][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["dpto"][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["munic"][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["acumpago"][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["retpago"][language] + ' </Data></Cell>' +
                '</Row>';

            //creacion de reporte xls
            for (var i = 0; i < arreglo.length; i++) {

                if (Math.abs(Number(arreglo[i][10]).toFixed(0)) > 0 || Math.abs(Number(arreglo[i][11]).toFixed(0)) > 0) {

                    xlsString += '<Row>';
                    //0. CONCEPTO
                    if (arreglo[i][0] != '' && arreglo[i][0] != null && arreglo[i][0] != '- None -') {
                        xlsString += '<Cell><Data ss:Type="String">' + arreglo[i][0] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //1. TDOC
                    if (arreglo[i][1] != '' && arreglo[i][1] != null && arreglo[i][1] != '- None -') {
                        xlsString += '<Cell><Data ss:Type="String">' + arreglo[i][1] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //2. NID
                    if (arreglo[i][9] != '' && arreglo[i][9] != null && arreglo[i][9] != '- None -') {
                        xlsString += '<Cell><Data ss:Type="String">' + arreglo[i][9] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //3. D.V.
                    if (arreglo[i][2] != '' && arreglo[i][2] != null && arreglo[i][2] != '- None -') {
                        xlsString += '<Cell><Data ss:Type="String">' + arreglo[i][2] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //4. 1ER APELL
                    if (arreglo[i][3] != '' && arreglo[i][3] != null && arreglo[i][3] != '- None -') {
                        if (arreglo[i][3] && arreglo[i][3].split(' ')[0]) {
                            xlsString += '<Cell><Data ss:Type="String">' + arreglo[i][3].split(' ')[0] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //5. 2DO Apellido
                    if (arreglo[i][3] != '' && arreglo[i][3] != null && arreglo[i][3] != '- None -') {
                        if (arreglo[i][3] && arreglo[i][3].split(' ')[1]) {
                            xlsString += '<Cell><Data ss:Type="String">' + arreglo[i][3].split(' ')[1] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //6. 1ER NOMBRE
                    if (arreglo[i][4] != '' && arreglo[i][4] != null && arreglo[i][4] != '- None -') {
                        if (arreglo[i][4] && arreglo[i][4].split(' ')[0]) {
                            xlsString += '<Cell><Data ss:Type="String">' + arreglo[i][4].split(' ')[0] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //7. 2DO NOMBRE
                    if (arreglo[i][4] != '' && arreglo[i][4] != null && arreglo[i][4] != '- None -') {
                        if (arreglo[i][4] && arreglo[i][4].split(' ')[1]) {
                            xlsString += '<Cell><Data ss:Type="String">' + arreglo[i][4].split(' ')[1] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //8. RAZON SOCIAL
                    if (arreglo[i][5] != '' && arreglo[i][5] != null && arreglo[i][5] != '- None -') {
                        xlsString += '<Cell><Data ss:Type="String">' + arreglo[i][5] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //9. DIRECCION
                    if (arreglo[i][8] != '' && arreglo[i][8] != null && arreglo[i][8] != '- None -') {
                        xlsString += '<Cell><Data ss:Type="String">' + arreglo[i][8] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //10. DEPTO
                    if (arreglo[i][6] != '' && arreglo[i][6] != null && arreglo[i][6] != '- None -') {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + arreglo[i][6] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
                    }
                    //11. NUM MUNICIPIO
                    if (arreglo[i][7] != '' && arreglo[i][7] != null && arreglo[i][7] != '- None -') {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + arreglo[i][7] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
                    }
                    //12. VALOR ACUMULADO DEL PAGO O ABONO SUJETO A RETENCION EN AL FUENTE
                    if (arreglo[i][10] != '' && arreglo[i][10] != null && arreglo[i][10] != '- None -') {
                        var auxcol12 = Number(arreglo[i][10]).toFixed(0);
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(auxcol12) + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
                    }
                    //13.  VALOR PAGOS NO COSTO NI DEDUCCION
                    if (arreglo[i][11] != '' && arreglo[i][11] != null && arreglo[i][11] != '- None -') {
                        var auxcol13 = Number(arreglo[i][11]).toFixed(0);
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(auxcol13) + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
                    }

                    xlsString += '</Row>';

                }
            } //fin del quiebre por clase


            xlsString += '</Table></Worksheet></Workbook>';


            strExcelVentasXPagar = encode.convert({
                string: xlsString,
                inputEncoding: encode.Encoding.UTF_8,
                outputEncoding: encode.Encoding.BASE_64
            });

            SaveFile(strExcelVentasXPagar, '.xls', NumEnvio);

        }

        function ObtenerCodigoCuenta(id_related, id_item, name_wht, tipo, monto) {
            if (tipo == 1) tipo_t = 'CustCred';
            if (tipo == 2) tipo_t = 'CustInvc';

            var busqueda_retencion = search.create({
                type: "transaction",
                filters: [
                    ["type", "anyof", "CustCred", "CustInvc"],
                    "AND", ["internalid", "anyof", id_related],
                    "AND", ["memomain", "startswith", name_wht],
                    "AND", ["item.internalid", "anyof", id_item]
                ],
                settings: [{
                    name: 'consolidationtype',
                    value: 'NONE'
                }],
                columns: [
                    search.createColumn({
                        name: "formulatext",
                        formula: "{account.custrecord_lmry_co_puc_concept}",
                        label: "Formula (Text)"
                    })
                ]
            });

            if (featureMultibook) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMultibook]
                });
                busqueda_retencion.filters.push(multibookFilter);

                var columna_idcuenta_2 = search.createColumn({
                    name: "formulanumeric",
                    formula: "ABS({accountingtransaction.amount})",
                    label: "id cuenta multibook"
                });
                busqueda_retencion.columns.push(columna_idcuenta_2);

                var columna_idcuenta = search.createColumn({
                    name: "formulanumeric",
                    formula: "{accountingtransaction.account.id}",
                    label: "id cuenta multibook"
                });
                busqueda_retencion.columns.push(columna_idcuenta);

            } else {
                var columna_idcuenta_2 = search.createColumn({
                    name: "formulanumeric",
                    formula: "ABS({amount})",
                    label: "monto"
                });
                busqueda_retencion.columns.push(columna_idcuenta_2);
            }
            var result = busqueda_retencion.run().getRange(0, 100);

            if (result.length != 0) {
                var columns = result[0].columns;
            } else {
                return '';
            }
            //ese segundo parametro no tengo ni puta idea de porque passa si lo borras abstente a las consecuencias, estba con dolor de cabez ese dia
            if (monto == 0) {
                if (featureMultibook) {


                    id_cuenta = result[0].getValue(columns[2]);
                    valor = VerdaderoCodigo(id_cuenta);
                    return valor;
                }
                valor = result[0].getValue(columns[0]);
                valores = valor.split(",");
                for (var i = 0; i < valores.length; i++) {
                    if (ArrConceptos.indexOf(valores[i].substring(0, 4)) != -1) {
                        valor = valores[i].substring(0, 4);
                        break;
                    }
                }
                return valor;
            }
            //esto de aca es cuando hacen la genial idea de crear con items iguales los tax result raaaaa
            if (featureMultibook) {
                if (result[0].getValue(columns[1]) == monto) {
                    id_cuenta = result[0].getValue(columns[2]);
                    //log.error('muestramelo nena',id_cuenta);
                    valor = VerdaderoCodigo(id_cuenta);
                    return valor;
                }
            } else {
                if (result[0].getValue(columns[1]) == monto) {
                    valor = result[0].getValue(columns[0]);
                    valores = valor.split(",");
                    for (var i = 0; i < valores.length; i++) {
                        if (ArrConceptos.indexOf(valores[i].substring(0, 4)) != -1) {
                            valor = valores[i].substring(0, 4);
                            break;
                        }
                    }
                }
                return '';
            }
            return '';
        }

        function ObtenerDatosCustomer(id_customer) {

            var search_customer = search.create({
                type: "customer",
                filters: [
                    ["internalid", "anyof", id_customer]
                ],
                columns: [
                    search.createColumn({
                        name: "formulatext",
                        //formula: "{custentity_tipo_doc_id_sunat.custrecord_tipo_doc_id}", estee s el id actual en edsa   el de abajo custentity_lmry_digito_verificator
                        formula: "{custentity_lmry_sunat_tipo_doc_id.custrecord_tipo_doc_id}",
                        label: "0. ID Documento "
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "CASE WHEN {custentity_lmry_sunat_tipo_doc_id.name} = 'NIT' THEN {custentity_lmry_digito_verificator} ELSE '' END",
                        label: "1. Digito Verfiicador"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "CASE WHEN {isperson}='T' THEN {lastname} ELSE '' END ",
                        label: "2. Apellidos"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "CASE WHEN {isperson}='T' THEN {firstname} ELSE '' END ",
                        label: "3. Nombres"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "CASE WHEN {isperson}='F' THEN {companyname} ELSE '' END ",
                        label: "4. Razon Social"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "SUBSTR({custentity_lmry_municcode}, 0, 2) ",
                        label: "5. Departamento"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "SUBSTR({custentity_lmry_municcode}, 3, 3)",
                        label: "6. Municipio"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{vatregnumber}",
                        label: "7. Numero Identificacion"
                    })
                ]
            });

            var resultados = search_customer.run().getRange(0, 100);
            var arrAuxiliar = [];

            var columns = resultados[0].columns;

            arrAuxiliar[0] = resultados[0].getValue(columns[0]);
            arrAuxiliar[1] = resultados[0].getValue(columns[1]).substring(0, 1);
            arrAuxiliar[2] = ValidaGuion(resultados[0].getValue(columns[2]));
            arrAuxiliar[3] = ValidaGuion(resultados[0].getValue(columns[3]));
            arrAuxiliar[4] = ValidaGuion(resultados[0].getValue(columns[4]));
            arrAuxiliar[5] = resultados[0].getValue(columns[5]);
            arrAuxiliar[6] = resultados[0].getValue(columns[6]);
            //esta espara la direccion
            arrAuxiliar[7] = '';
            arrAuxiliar[8] = ValidaGuion(resultados[0].getValue(columns[7]));

            direccion = search.lookupFields({
                type: search.Type.CUSTOMER,
                id: id_customer,
                columns: ['billingaddress.address1']
            });
            if (direccion["billingaddress.address1"] != null && direccion["billingaddress.address1"] != '') {
                arrAuxiliar[7] = direccion["billingaddress.address1"];
            }

            return arrAuxiliar;
        }

        function ValidaGuion(s) {
            var AccChars = "+./-(),&#¢∞¬!$=?%&[]\\";
            var RegChars = "";
            s = String(s);
            for (var c = 0; c < s.length; c++) {
                for (var special = 0; special < AccChars.length; special++) {
                    if (s.charAt(c) == AccChars.charAt(special)) {
                        s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
                    }
                }
            }
            return s;
        }

        function NoData() {
            var record = recordModulo.load({
                type: 'customrecord_lmry_co_rpt_generator_log',
                id: paramIDRecord
            });

            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: 'No existe informacion para los criterios seleccionados.'
            });
            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_postingperiod',
                value: paramPeriodo
            });

            var recordId = record.save();
        }

        function lengthInUtf8Bytes(str) {
            var m = encodeURIComponent(str).match(/%[89ABab]/g);
            return str.length + (m ? m.length : 0);
        }


        function VerdaderoCodigo(id_cuenta) {
            var result = [0, 0];
            var accountSearchObj = search.create({
                type: "account",
                filters: [
                    ["internalid", "anyof", id_cuenta],
                    'AND', ["formulatext: {custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_format_c.id}", "is", "2"]
                ],
                columns: [
                    search.createColumn({
                        name: "formulatext",
                        formula: "{custrecord_lmry_co_puc_concept}",
                        label: "Formula (Text)"
                    }),
                    search.createColumn({
                        name: "custrecord_lmry_co_puc_format_c",
                        join: "custrecord_lmry_co_puc_concept"
                    })
                ]
            });

            valor = accountSearchObj.run().getRange(0, 100);
            var columns = valor[0].columns;

            valores = valor[0].getValue(columns[0]).split(",");
            //log.error('valor columna 1 verdcod es: ', valor[0].getValue(columns[1]));
            for (var i = 0; i < valores.length; i++) {
                if (valor[0].getValue(columns[1]) == 2) {
                    if (valores[i].substring(0, 2) == '13') {
                        result[0] = valores[i].substring(0, 4);
                        result[1] = valor[0].getValue(columns[1]);
                        break;
                    }
                }
            }
            log.debug('el return cuentas es: ', result);
            return result;
        }

        function ValidarAcentos(s) {
            var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ°–—ªº·";
            var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyo--ao.";

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

        function ValidarAcentos2(s) {
            var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ°–—ªº·-,_.";
            var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyo--ao.    ";

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

        function Name_File(numeroEnvio) {
            var name = '';

            name = 'Dmuisca_' + completar_cero(2, paramConcepto) + '01003' + '10' + paramPeriodo + completar_cero(8, numeroEnvio);

            return name;
        }

        function completar_cero(long, valor) {

            if ((('' + valor).length) <= long) {
                if (long != ('' + valor).length) {
                    for (var i = (('' + valor).length); i < long; i++) {
                        valor = '0' + valor;
                    }
                } else {
                    return valor;
                }
                return valor;
            } else {
                valor = valor.substring(0, long);
                return valor;
            }

        }


        function SaveFile(contenido, extension, NumEnvio) {

            ObtenerParametrosYFeatures();
            ObtenerDatosSubsidiaria();
            var generarXml = false;
            var folderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co'
            });

            // Almacena en la carpeta de Archivos Generados
            if (folderId != '' && folderId != null) {
                // Extension del archivo

                var fileName = Name_File(NumEnvio) + extension;

                // Crea el archivo
                var ventasXPagarFile;

                if (extension == '.xls') {
                    ventasXPagarFile = fileModulo.create({
                        name: fileName,
                        fileType: fileModulo.Type.EXCEL,
                        contents: contenido,
                        folder: folderId
                    });


                } else {
                    generarXml = true;
                    ventasXPagarFile = fileModulo.create({
                        name: fileName,
                        fileType: fileModulo.Type.PLAINTEXT,
                        contents: contenido,
                        encoding: fileModulo.Encoding.ISO_8859_1,
                        folder: folderId
                    });
                }

                var fileId = ventasXPagarFile.save();

                ventasXPagarFile = fileModulo.load({
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

                if (fileId) {
                    var usuario = runtime.getCurrentUser();
                    var employee = search.lookupFields({
                        type: search.Type.EMPLOYEE,
                        id: usuario.id,
                        columns: ['firstname', 'lastname']
                    });
                    var usuarioName = employee.firstname + ' ' + employee.lastname;

                    if (generarXml) {
                        var recordLog = recordModulo.create({
                            type: 'customrecord_lmry_co_rpt_generator_log'
                        });
                    } else {
                        var recordLog = recordModulo.load({
                            type: 'customrecord_lmry_co_rpt_generator_log',
                            id: paramIDRecord
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
                        value: namereport
                    });

                    //Nombre de Subsidiaria
                    recordLog.setValue({
                        fieldId: 'custrecord_lmry_co_rg_subsidiary',
                        value: companyName
                    });

                    //Periodo
                    recordLog.setValue({
                        fieldId: 'custrecord_lmry_co_rg_postingperiod',
                        value: periodName
                    });

                    if (featureMultibook) {
                        //Multibook
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
                    //libreria.sendrptuser(reportName, 3, fileName);
                }
            } else {
                log.error({
                    title: 'Creacion de File:',
                    details: 'No existe el folder'
                });
            }
        }

        function ObtenerParametrosYFeatures() {

            //periodStartDate = new Date(paramPeriodo, 0, 1);
            periodStartDate = new Date(paramPeriodo, 0, 1);
            periodEndDate = new Date(paramPeriodo, 11, 31);

            periodStartDate = format.format({
                value: periodStartDate,
                type: format.Type.DATE
            });

            periodEndDate = format.format({
                value: periodEndDate,
                type: format.Type.DATE
            });

            periodName = paramPeriodo;

            if (featureMultibook) {
                //Multibook Name
                var multibookName_temp = search.lookupFields({
                    type: search.Type.ACCOUNTING_BOOK,
                    id: paramMultibook,
                    columns: ['name']
                });
                multibookName = multibookName_temp.name;
            }

            licenses = libFeature.getLicenses(paramSubsidiaria);
            featureCabecera = libFeature.getAuthorization(27, licenses);
            featureLineas = libFeature.getAuthorization(340, licenses);

            //log.error('haber esos feature',featureCabecera + '---'+featureLineas );
        }

        function ObtenerDatosSubsidiaria() {
            var configpage = config.load({
                type: config.Type.COMPANY_INFORMATION
            });

            if (featureSubsidiaria) {
                companyName = ObtainNameSubsidiaria(paramSubsidiaria);

                companyRuc = ObtainFederalIdSubsidiaria(paramSubsidiaria);
            } else {
                companyRuc = configpage.getFieldValue('employerid');
                companyName = configpage.getFieldValue('legalname');
            }
            companyName = ValidaGuion(companyName);
            companyRuc = companyRuc.replace(' ', '');
        }

        function ObtainNameSubsidiaria(subsidiary) {
            try {
                if (subsidiary != '' && subsidiary != null) {
                    var subsidyName = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: subsidiary,
                        columns: ['legalname']
                    });

                    return subsidyName.legalname
                }
            } catch (err) {
                libreria.sendMail(LMRY_script, ' [ ObtainNameSubsidiaria ] ' + err);
            }
            return '';
        }

        function ObtainFederalIdSubsidiaria(subsidiary) {
            try {
                if (subsidiary != '' && subsidiary != null) {
                    var federalId = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: subsidiary,
                        columns: ['taxidnum']
                    });

                    return federalId.taxidnum
                }
            } catch (err) {
                libreria.sendMail(LMRY_script, ' [ ObtainFederalIdSubsidiaria ] ' + err);
            }
            return '';
        }




        function ObtenerCFOPAmountICMS(invoice, unique, tipo) {
            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;
            var vendorbillSearchObj = search.create({
                type: "invoice",
                filters: [
                    ["type", "anyof", "CustInvc"],
                    "AND", ["mainline", "is", "F"],
                    "AND", ["posting", "is", "T"],
                    "AND", ["memorized", "is", "F"],
                    "AND", ["voided", "is", "F"],
                    "AND", ["internalid", "anyof", invoice],
                    "AND", ["lineuniquekey", "equalto", unique]
                ],
                columns: [
                    search.createColumn({ name: "item", label: "Item" }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{custcol_lmry_br_tran_outgoing_cfop}",
                        label: "CFOP"

                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{custcol_lmry_br_cst_icms.custrecord_lmry_br_tax_situacion_code}",
                        label: "ICMS"

                    }),
                    search.createColumn({ name: "grossamount", label: "Amount (Gross)" })
                ],
                settings: [{
                    name: 'consolidationtype',
                    value: 'NONE'
                }]
            });
            var searchresult = vendorbillSearchObj.run();

            var Data = '';
            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
                if (objResult != null) {
                    var intLength = objResult.length;
                    if (intLength != 1000) {
                        DbolStop = true;
                    }
                    var arrPrueba = [];
                    for (var i = 0; i < intLength; i++) {
                        var Arrtemporal = [];
                        var columns = objResult[i].columns;
                        var columna0 = objResult[i].getValue(columns[0]);
                        var columna1 = objResult[i].getValue(columns[1]);
                        var columna2 = objResult[i].getValue(columns[2]);
                        var columna3 = objResult[i].getValue(columns[3]);
                        arrPrueba = [columna0, columna1, columna2, columna3];
                    }
                    if (!DbolStop) {
                        intDMinReg = intDMaxReg;
                        intDMaxReg += 1000;
                    }
                } else {
                    DbolStop = true;
                }
            }
            if (tipo == '1') {
                var banderita = columna1;
            } else if (tipo == '2') {
                var banderita = columna2;
            } else if (tipo == '3') {
                var banderita = columna3;
            }
            return banderita;
        }

        function ObtenerTransaccionesCMI() {

            var cont = 0;
            var arrReturn = new Array();
            var DbolStop = false;
            var infoTxt = '';



            //busqueda para retenciones de credit memo o invoice
            var searchload = search.load({
                id: 'customsearch_lmry_co_form1003_v10_3'
            });
            log.debug('Parametros', paramSubsidiaria + '->' + periodStartDate + '->' + periodEndDate);
            if (featureSubsidiaria) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidiaria]
                });
                searchload.filters.push(subsidiaryFilter);
            }

            if (paramPeriodo) {
                var fechInicioFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORAFTER,
                    values: [periodStartDate]
                });
                searchload.filters.push(fechInicioFilter);
                var fechFinFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORBEFORE,
                    values: [periodEndDate]
                });
                searchload.filters.push(fechFinFilter);
            }

            //columna 8  id de JOBS
            var columnaIDCustomer = search.createColumn({
                name: "formulanumeric",
                formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
                label: "7. ID Customer"
            });
            searchload.columns.push(columnaIDCustomer);
            log.debug('paramMultibook', paramMultibook);
            if (featureMultibook) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMultibook]
                });
                searchload.filters.push(multibookFilter);

                //9. monto multibook
                var colmuna_rate = search.createColumn({
                    name: "amount",
                    join: "accountingTransaction",
                    label: "monto "
                });
                searchload.columns.push(colmuna_rate);

                //10. cuenta multibook
                var colmuna_rate = search.createColumn({
                    name: "formulanumeric",
                    formula: "{accountingtransaction.account.id}",
                    label: "id cuenta multibook"
                });
                searchload.columns.push(colmuna_rate);
            }

            //11. Memo Main
            var memoMain = search.createColumn({
                name: "formulatext",
                formula: "{memomain}",
                label: "memomain"
            });
            searchload.columns.push(memoMain);

            //12. Internal ID
            var internalID = search.createColumn({
                name: "formulanumeric",
                formula: "{internalid}",
                label: "internalid"
            });
            searchload.columns.push(internalID);

            var pagedData = searchload.runPaged({
                pageSize: 1000
            });
            var resultArray = new Array();
            var page, auxArray, columns;

            pagedData.pageRanges.forEach(function(pageRange) {
                page = pagedData.fetch({
                    index: pageRange.index
                });

                page.data.forEach(function(result) {
                    columns = result.columns;
                    auxArray = [];

                    //0 id customer
                    if ((feature_Project && !feature_magProject) || (feature_Project && feature_magProject)) {
                        auxArray[1] = result.getValue(columns[8]);

                    } else {
                        auxArray[1] = result.getValue(columns[0]);
                    }
                    //1. departamento
                    auxArray[0] = result.getValue(columns[1]);
                    //2. municipio
                    auxArray[2] = result.getValue(columns[2]);
                    //3. id transaccion referenciada
                    auxArray[3] = result.getValue(columns[3]);
                    //4. monto multibook
                    if (featureMultibook) {
                        auxArray[4] = result.getValue(columns[9]);
                    } else {
                        auxArray[4] = result.getValue(columns[4]);
                    }
                    if (featureMultibook) {
                        //cuenta
                        auxArray[5] = result.getValue(columns[10]);
                    } else {
                        //cuenta
                        auxArray[5] = result.getValue(columns[5]);
                    }
                    //6. es linea?
                    auxArray[6] = result.getValue(columns[6]);
                    //7. tipo de transaccion relacionado
                    auxArray[7] = result.getValue(columns[7]);
                    //8. Memo Main
                    if (featureMultibook) {
                        auxArray[8] = result.getValue(columns[11]);
                    } else {
                        auxArray[8] = result.getValue(columns[9]);
                    }
                    //9. InternalID
                    if (featureMultibook) {
                        auxArray[9] = result.getValue(columns[12]);
                    } else {
                        auxArray[9] = result.getValue(columns[10]);
                    }

                    arrReturn.push(auxArray);
                });

            });
            /*var searchload = search.load({
                id: 'customsearch_lmry_co_form1003_v10_3'
            });
            if (featureSubsidiaria) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidiaria]
                });
                searchload.filters.push(subsidiaryFilter);
            }
      
            if (paramPeriodo) {
              var fechInicioFilter = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORAFTER,
                values: [periodStartDate]
              });
              searchload.filters.push(fechInicioFilter);
              var fechFinFilter = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORBEFORE,
                values: [periodEndDate]
              });
              searchload.filters.push(fechFinFilter);
            }
      
            /*if (featureCabecera) {
              var filtro_monto_base = search.createFilter({
                name: "custbody_lmry_co_retefte_amount",
                operator: search.Operator.GREATERTHAN,
                values: [0]
              });
              searchload.filters.push(filtro_monto_base);
      
              var filtro_retefte = search.createFilter({
                name: "custbody_lmry_co_retefte",
                operator: search.Operator.ISNOTEMPTY
              });
              searchload.filters.push(filtro_monto_base);
      
              //filtro sobre los aplicados o implicados raaaaaa
              var filtro_apply = search.createFilter({
                name: "formulatext",
                formula: "CASE WHEN CONCAT('',{type.id}) = 'CustInvc' THEN (CASE WHEN {applyingtransaction.memomain} = CONCAT('Latam - WHT ',{custbody_lmry_co_retefte}) THEN 1 ELSE 0 END) ELSE (CASE WHEN {appliedtotransaction.memomain} = CONCAT('Latam - WHT ',{custbody_lmry_co_retefte}) THEN 1 ELSE 0 END) END",
                operator: search.Operator.IS,
                values: ['1']
              });
              searchload.filters.push(filtro_apply);
      
            }else {
              var filtro_tax_result = search.createFilter({
                name: "formulatext",
                formula: "{custrecord_lmry_br_transaction.custrecord_lmry_br_type}",
                operator: search.Operator.IS,
                values: ["ReteFTE"]
              });
              searchload.filters.push(filtro_tax_result);
      
              //filtro sobre los aplicados o implicados raaaaaa
              var filtro_apply = search.createFilter({
                name: "formulatext",
                formula: "CASE WHEN CONCAT('',{type.id}) = 'CustInvc' THEN (CASE WHEN {applyingtransaction.memomain} = 'Latam - Country WHT (Lines) - ReteFTE' THEN 1 ELSE 0 END) ELSE (CASE WHEN {appliedtotransaction.memomain} = 'Latam - Country WHT (Lines) - ReteFTE' THEN 1 ELSE 0 END) END",
                operator: search.Operator.IS,
                values: ['1']
              });
              searchload.filters.push(filtro_apply);
      
            }
            //columna 15  id de JOBS
            var columnaIDCustomer = search.createColumn({
               name: "formulanumeric",
               formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
               label: "23. ID Customer"
             });
             searchload.columns.push(columnaIDCustomer);
      
            if (featureMultibook) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMultibook]
                });
                searchload.filters.push(multibookFilter);
      
                //16. miralo
                var colmuna_rate = search.createColumn({
                   name: "exchangerate",
                   join: "accountingTransaction",
                   label: "Exchange Rate"
                });
                searchload.columns.push(colmuna_rate);
            }
      
            var pagedData = searchload.runPaged({
                pageSize : 1000
            });
            var resultArray = new Array();
            var page, auxArray, columns;
      
            pagedData.pageRanges.forEach(function(pageRange) {
                page = pagedData.fetch({
                    index : pageRange.index
                });
                page.data.forEach(function(result) {
                    columns = result.columns;
                    auxArray = [];
      
                    //0. Internal ID
                    auxArray[0] =  result.getValue(columns[0]);
                    //1. Customer ID
                    if ((feature_Project && !feature_magProject) || (feature_Project && feature_magProject)) {
                      auxArray[1] = result.getValue(columns[15]);
      
                    }else {
                      auxArray[1] = result.getValue(columns[1]);
                    }
                    //2. departamento
                    if (result.getValue(columns[2]) != '' && result.getValue(columns[2]) != '- None -' && result.getValue(columns[2]) != null) {
                      auxArray[2] =  result.getValue(columns[2]);
                    }else {
                      auxArray[2] = '';
                    }
                    //3. municipio
                    if (result.getValue(columns[3]) != '' && result.getValue(columns[3]) != '- None -' && result.getValue(columns[3]) != null) {
                      auxArray[3] =  result.getValue(columns[3]);
                    }else {
                      auxArray[3] = '';
                    }
                    //4. Retencion del total
                    auxArray[4] =  result.getValue(columns[5])*1;
                    if (result.getValue(columns[13]) == 'CustCred') {
                    auxArray[4] = -1*auxArray[4]
                    }
                    //5. Base de la Retencion
                    if (result.getValue(columns[6]) == 1) {
                      if (featureMultibook) {
                        auxArray[5] = result.getValue(columns[9])*result.getValue(columns[16])/result.getValue(columns[10]);
                      }else {
                        auxArray[5] = result.getValue(columns[9]);
                      }
                    }else if (result.getValue(columns[6]) == 2) {
                      if (featureMultibook) {
                        auxArray[5] = result.getValue(columns[8])*result.getValue(columns[16])/result.getValue(columns[10]);
                      }else {
                        auxArray[5] = result.getValue(columns[8]);
                      }
                    }else if (result.getValue(columns[6]) == 3) {
                      if (featureMultibook) {
                        auxArray[5] = result.getValue(columns[7])*result.getValue(columns[16])/result.getValue(columns[10]);
                      }else {
                        auxArray[5] = result.getValue(columns[7]);
                      }
                    }else {
                      if (featureMultibook) {
                        auxArray[5] = result.getValue(columns[8])*result.getValue(columns[16])/result.getValue(columns[10]);
                      }else {
                        auxArray[5] = result.getValue(columns[8]);
                      }
                    }
                    //6. Internal ID - Credit Memo
                    auxArray[6] = result.getValue(columns[11])
                    //7. Internal ID - Invoice
                    auxArray[7] = result.getValue(columns[12])
                    //8. tipo de transaccion
                    auxArray[8] = result.getValue(columns[13]);
                    //9. Account de la transccion si no tiene tax result
                    auxArray[9] = result.getValue(columns[14]);
                    //10. Wht code
                    auxArray[10] = result.getValue(columns[4]);
                    arrReturn.push(auxArray);
      
                });
            });*/

            return arrReturn;
        }

        function obtenerReteJournals() {
            var arrReturn = new Array();

            //busqueda para retenciones sobre journals
            var searchJournal = search.load({ id: "customsearch_lmry_co_1003_mm_journal" });

            if (featureSubsidiaria) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidiaria]
                });
                searchJournal.filters.push(subsidiaryFilter);
            }

            if (featureMultibook) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMultibook]
                });
                searchJournal.filters.push(multibookFilter);

                var columna_idcuenta = search.createColumn({
                    name: "formulanumeric",
                    formula: "{accountingtransaction.account.id}",
                    summary: "GROUP",
                    label: "columna de id "
                });
                searchJournal.columns.push(columna_idcuenta);
            } else {

                var columna_idcuenta = search.createColumn({
                    name: "formulanumeric",
                    formula: "{account.internalid}",
                    summary: "GROUP",
                    label: "columna de id "
                });
                searchJournal.columns.push(columna_idcuenta);
            }
            var type_id = search.createColumn({
                name: "formulatext",
                formula: "CONCAT('',{custbody_lmry_reference_transaction.type.id})",
                summary: "GROUP",
                label: "columna de id "
            });
            searchJournal.columns.push(type_id);

            if (paramPeriodo) {
                var fechInicioFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORAFTER,
                    values: [periodStartDate]
                });
                searchJournal.filters.push(fechInicioFilter);
                var fechFinFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORBEFORE,
                    values: [periodEndDate]
                });
                searchJournal.filters.push(fechFinFilter);
            }

            if (true) {
                var filtro_monto_base = search.createFilter({
                    name: "formulatext",
                    formula: "CASE WHEN CONCAT('Latam - WHT ',{custbody_lmry_reference_transaction.custbody_lmry_co_retefte}) = {memomain} OR CONCAT('Latam - WHT Reclasification ',{custbody_lmry_reference_transaction.custbody_lmry_co_retefte}) = {memomain} OR {memomain} = 'Latam - Country WHT (Lines) - ReteFTE' OR {memomain} = 'Latam - Country WHT (Lines) Reclasification - ReteFTE' OR {memomain} = 'Latam - Country WHT (Lines) - Auto ReteFTE' OR {memomain} = 'Latam - Country WHT (Lines) Reclasification - Auto ReteFTE' OR {memomain} = 'Latam - CO WHT (Lines) - Auto ReteFTE' OR {memomain} = 'Latam - CO WHT (Lines) Reclasification - Auto ReteFTE' OR {memomain} = 'Latam - CO WHT (Lines) - ReteFTE' OR {memomain} = 'Latam - CO WHT (Lines) Reclasification - ReteFTE' THEN 1 ELSE 0 END",
                    operator: search.Operator.IS,
                    values: ["1"]
                });
                searchJournal.filters.push(filtro_monto_base);
            }

            var internal_id = search.createColumn({
                name: "formulanumeric",
                formula: "{internalid}",
                summary: "GROUP",
                label: "columna de id "
            });
            searchJournal.columns.push(internal_id);

            var pagedData = searchJournal.runPaged({
                pageSize: 1000
            });

            //log.error('pagedData',pagedData);
            var page, auxArray, columns, objResult;
            pagedData.pageRanges.forEach(function(pageRange) {
                page = pagedData.fetch({
                    index: pageRange.index
                });

                page.data.forEach(function(objResult) {
                    columns = objResult.columns;

                    auxArray = [];
                    //tipo transaccion
                    auxArray[0] = objResult.getValue(columns[8]);
                    //1. Customer ID
                    auxArray[1] = objResult.getValue(columns[0]);
                    //2. Memo para Linea
                    if (objResult.getValue(columns[1]) != '' && objResult.getValue(columns[1]) != '- None -' && objResult.getValue(columns[1]) != null) {
                        auxArray[2] = objResult.getValue(columns[1]);
                    } else {
                        auxArray[2] = '';
                    }
                    //3. memo para el total
                    if (objResult.getValue(columns[2]) != '' && objResult.getValue(columns[2]) != '- None -' && objResult.getValue(columns[2]) != null) {
                        auxArray[3] = objResult.getValue(columns[2]);
                    } else {
                        auxArray[3] = '';
                    }
                    //4. ID_transactoon_referencia
                    auxArray[4] = objResult.getValue(columns[3]);
                    //5. concepto medio magnetico
                    if (objResult.getValue(columns[4]) != '' && objResult.getValue(columns[4]) != '- None -' && objResult.getValue(columns[4]) != null) {
                        auxArray[5] = objResult.getValue(columns[4]);

                    } else {
                        auxArray[5] = '';
                    }

                    //5. ID Retefuente Total
                    if (objResult.getValue(columns[5]) != '' && objResult.getValue(columns[5]) != '- None -' && objResult.getValue(columns[5]) != null) {
                        auxArray[6] = objResult.getValue(columns[5]);
                    } else {
                        auxArray[6] = '';
                    }
                    //7. Id de la cuenta
                    auxArray[7] = objResult.getValue(columns[7]);
                    //8. Rete Cabecera o Linea
                    auxArray[8] = objResult.getValue(columns[6]);
                    //9. Internal ID Journal
                    auxArray[9] = objResult.getValue(columns[9]);
                    //10. Uno más pa que no descuadre
                    auxArray[10] = 'nada';

                    arrReturn.push(auxArray);
                });
            });

            return arrReturn;
        }

        function obtenerJournalsAMano() {
            var arrReturn = new Array();

            //JOURNALS HECHOS A MANO
            var search_journal = search.create({
                type: "journalentry",
                filters: [
                    ["type", "anyof", "Journal"],
                    "AND", ["formulatext: {customer.internalid}", "isnotempty", ""],
                    "AND", ["posting", "is", "T"]
                ],
                columns: [
                    search.createColumn({
                        name: "internalid",
                        summary: "GROUP",
                        sort: search.Sort.ASC,
                        label: "0.Internal ID"
                    }),
                    search.createColumn({
                        name: "lineuniquekey",
                        summary: "GROUP",
                        label: "1.Line Unique Key"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: "{account.custrecord_lmry_co_puc_concept}",
                        label: "2.Formula (Text)"
                    }),
                    search.createColumn({
                        name: "formulanumeric",
                        summary: "SUM",
                        formula: " NVL({debitamount},0)",
                        label: "3.Formula (Numeric)"
                    }),
                    search.createColumn({
                        name: "formulanumeric",
                        summary: "GROUP",
                        formula: "{customer.internalid}",
                        label: "4. cust"
                    })
                ]
            });

            if (featureSubsidiaria) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidiaria]
                });
                search_journal.filters.push(subsidiaryFilter);
            }

            if (paramPeriodo) {
                var fechInicioFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORAFTER,
                    values: [periodStartDate]
                });
                search_journal.filters.push(fechInicioFilter);

                var fechFinFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORBEFORE,
                    values: [periodEndDate]
                });
                search_journal.filters.push(fechFinFilter);
            }

            if (featureMultibook) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMultibook]
                });
                search_journal.filters.push(multibookFilter);

                //columna de id cuenta multibook
                var colmuna_rate = search.createColumn({
                    name: "formulanumeric",
                    summary: "GROUP",
                    formula: "{accountingtransaction.account.id}",
                    label: " 5 Formula (Numeric)"
                });
                search_journal.columns.push(colmuna_rate);
            } else {
                var colmuna_rate = search.createColumn({
                    name: "formulanumeric",
                    summary: "GROUP",
                    formula: "{account.internalid}",
                    label: " 5 Formula (Numeric)"
                });
                search_journal.columns.push(colmuna_rate);

            }
            var pagedData = search_journal.runPaged({
                pageSize: 1000
            });
            var page, auxArray, columns;
            var arreglo = [];

            pagedData.pageRanges.forEach(function(pageRange) {
                page = pagedData.fetch({
                    index: pageRange.index
                });
                page.data.forEach(function(result) {
                    columns = result.columns;
                    auxArray = [];
                    //0. internal id journal
                    auxArray[0] = result.getValue(columns[0]);
                    //1. id customer
                    auxArray[1] = result.getValue(columns[4]);
                    //2. es linea de debito o credito
                    auxArray[2] = result.getValue(columns[3]);
                    //3. cuenta no multibook

                    auxArray[3] = result.getValue(columns[5]);

                    //4. line unique key
                    auxArray[4] = result.getValue(columns[1]);
                    arrReturn.push(auxArray);
                });
            });

            return arrReturn;
        }

        function getGlobalLabels() {
            var labels = {
                "titulo": {
                    "es": 'FORMULARIO 1003: RETENCIONES EN LA FUENTE QUE LE PRACTICARON',
                    "pt": 'FORMA 1003: RETENÇÕES NA FONTE PRATICADA',
                    "en": 'FORM 1003: WITHHOLDINGS AT THE SOURCE PRACTICED'
                },
                "razSocial": {
                    "es": 'Razon Social',
                    "pt": 'Razão social',
                    "en": 'Company name'
                },
                "taxNumber": {
                    "es": 'Numero de Impuesto',
                    "pt": 'Número de identificação fiscal',
                    "en": 'Tax Number'
                },
                "multib": {
                    "es": 'Multilibro',
                    "pt": 'Multibook',
                    "en": 'Multibook'
                },
                "period": {
                    "es": 'Periodo',
                    "pt": 'Período',
                    "en": 'Period'
                },
                "al": {
                    "es": ' al ',
                    "pt": ' a ',
                    "en": ' to '
                },
                "cpt": {
                    "es": 'Concepto',
                    "pt": 'Concepto',
                    "en": 'Concept'
                },
                "primerApe": {
                    "es": '1er Apelli',
                    "pt": '1er Apelli',
                    "en": '1st Last Name'
                },
                "segundApe": {
                    "es": '2do Apelli',
                    "pt": '2do Apelli',
                    "en": '2nd Last Name'
                },
                "primerNom": {
                    "es": '1er Nombre',
                    "pt": '1er Nombre',
                    "en": '1st Name'
                },
                "segundNom": {
                    "es": '2do Nombre',
                    "pt": '2do Nombre',
                    "en": '2st Name'
                },
                "direc": {
                    "es": 'Direccion',
                    "pt": 'Endereço',
                    "en": 'Address'
                },
                "dpto": {
                    "es": 'Departamento',
                    "pt": 'Departamento',
                    "en": 'Department'
                },
                "munic": {
                    "es": 'Municipio',
                    "pt": 'Município',
                    "en": 'Municipality'
                },
                "acumpago": {
                    "es": 'Valor Acum. Pago',
                    "pt": 'Accum. Pagamento',
                    "en": 'Accum. Payment'
                },
                "retpago": {
                    "es": 'Retencion Practicado',
                    "pt": 'Retenção Prática',
                    "en": 'Retention Practiced'
                },
                "origin": {
                    "es": "Origen",
                    "en": "Origin",
                    "pt": "Origem"
                },
                "date": {
                    "es": "Fecha",
                    "en": "Date",
                    "pt": "Data"
                },
                "time": {
                    "es": "Hora",
                    "en": "Time",
                },          

            };

            return labels;
        }

        function ObtenerDatosTaxResultRJL(id_transaction, id_journal, type) {
            var jsonTemp = {};
            var arrayIDs = ObtenerDatosTaxResultReclasification(id_transaction, id_journal, type);
            log.debug('el arrayIds es: ', arrayIDs);

            if (type == 1) {

                var datosTaxResultRJL = search.create({
                    type: "customrecord_lmry_br_transaction",
                    filters: [
                        ["formulatext: CASE WHEN NVL({custrecord_lmry_br_type},'') = 'ReteFTE' OR NVL({custrecord_lmry_br_type},'') = 'Auto ReteFTE' THEN 1 ELSE 0 END", "is", "1"],
                        "AND", ["custrecord_lmry_br_transaction.mainline", "is", "T"],
                        "AND", ["custrecord_lmry_br_transaction.internalid", "anyof", id_transaction]
                    ],
                    columns: [
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ntax.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ntax.custrecord_lmry_ntax_credit_account.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ntax.custrecord_lmry_ntax_debit_account.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ccl.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ccl.custrecord_lmry_br_ccl_account2.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ccl.custrecord_lmry_br_ccl_account1.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "NVL({custrecord_lmry_base_amount_local_currc},0)",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "NVL({custrecord_lmry_amount_local_currency},0)",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{internalid}",
                            label: "Formula (Numeric)"
                        })
                    ]
                });
                var pagedData = datosTaxResultRJL.runPaged({
                    pageSize: 1000
                });

                var page, columns;

                pagedData.pageRanges.forEach(function(pageRange) {
                    page = pagedData.fetch({
                        index: pageRange.index
                    });

                    page.data.forEach(function(result) {
                        columns = result.columns;

                        if (arrayIDs.length != 0) {
                            if (arrayIDs.indexOf(Number(result.getValue(columns[8]))) == -1) {

                                if (result.getValue(columns[0]) != '') {
                                    var keyNT = 'NT_' + result.getValue(columns[0]);
                                    if (jsonTemp[keyNT] == undefined) {
                                        jsonTemp[keyNT] = result.getValue(columns[1]) + '|' + result.getValue(columns[2]) + '|' + result.getValue(columns[6]) + '|' + result.getValue(columns[7]);
                                    } else {
                                        var tempJK = jsonTemp[keyNT].split('|');
                                        var tempMB = Number(tempJK[2]) + Number(result.getValue(columns[6]));
                                        var tempRT = Number(tempJK[3]) + Number(result.getValue(columns[7]));
                                        jsonTemp[keyNT] = tempJK[0] + '|' + tempJK[1] + '|' + tempMB + '|' + tempRT;
                                    }
                                } else {
                                    var keyNT = 'CC_' + result.getValue(columns[3]);
                                    if (jsonTemp[keyNT] == undefined) {
                                        jsonTemp[keyNT] = result.getValue(columns[4]) + '|' + result.getValue(columns[5]) + '|' + result.getValue(columns[6]) + '|' + result.getValue(columns[7]);
                                    } else {
                                        var tempJK = jsonTemp[keyNT].split('|');
                                        var tempMB = Number(tempJK[2]) + Number(result.getValue(columns[6]));
                                        var tempRT = Number(tempJK[3]) + Number(result.getValue(columns[7]));
                                        jsonTemp[keyNT] = tempJK[0] + '|' + tempJK[1] + '|' + tempMB + '|' + tempRT;
                                    }
                                }
                            }
                        } else {
                            if (result.getValue(columns[0]) != '') {
                                var keyNT = 'NT_' + result.getValue(columns[0]);
                                if (jsonTemp[keyNT] == undefined) {
                                    jsonTemp[keyNT] = result.getValue(columns[1]) + '|' + result.getValue(columns[2]) + '|' + result.getValue(columns[6]) + '|' + result.getValue(columns[7]);
                                } else {
                                    var tempJK = jsonTemp[keyNT].split('|');
                                    var tempMB = Number(tempJK[2]) + Number(result.getValue(columns[6]));
                                    var tempRT = Number(tempJK[3]) + Number(result.getValue(columns[7]));
                                    jsonTemp[keyNT] = tempJK[0] + '|' + tempJK[1] + '|' + tempMB + '|' + tempRT;
                                }
                            } else {
                                var keyNT = 'CC_' + result.getValue(columns[3]);
                                if (jsonTemp[keyNT] == undefined) {
                                    jsonTemp[keyNT] = result.getValue(columns[4]) + '|' + result.getValue(columns[5]) + '|' + result.getValue(columns[6]) + '|' + result.getValue(columns[7]);
                                } else {
                                    var tempJK = jsonTemp[keyNT].split('|');
                                    var tempMB = Number(tempJK[2]) + Number(result.getValue(columns[6]));
                                    var tempRT = Number(tempJK[3]) + Number(result.getValue(columns[7]));
                                    jsonTemp[keyNT] = tempJK[0] + '|' + tempJK[1] + '|' + tempMB + '|' + tempRT;
                                }
                            }
                        }
                    });

                });

                log.debug('el jsontemp es: ', jsonTemp);

            } else {
                var datosTaxResultRJL = search.create({
                    type: "customrecord_lmry_br_transaction",
                    filters: [
                        ["formulatext: CASE WHEN NVL({custrecord_lmry_br_type},'') = 'ReteFTE' OR NVL({custrecord_lmry_br_type},'') = 'Auto ReteFTE' THEN 1 ELSE 0 END", "is", "1"],
                        "AND", ["custrecord_lmry_br_transaction.mainline", "is", "T"],
                        "AND", ["custrecord_lmry_br_transaction.internalid", "anyof", id_transaction]
                    ],
                    columns: [
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ntax.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ntax.custrecord_lmry_ntax_credit_account.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ntax.custrecord_lmry_ntax_debit_account.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ccl.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ccl.custrecord_lmry_br_ccl_account2.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_ccl.custrecord_lmry_br_ccl_account1.id}",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "NVL({custrecord_lmry_base_amount_local_currc},0)",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "NVL({custrecord_lmry_amount_local_currency},0)",
                            label: "Formula (Numeric)"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{internalid}",
                            label: "Formula (Numeric)"
                        })
                    ]
                });
                var pagedData = datosTaxResultRJL.runPaged({
                    pageSize: 1000
                });

                var page, columns;

                pagedData.pageRanges.forEach(function(pageRange) {
                    page = pagedData.fetch({
                        index: pageRange.index
                    });

                    page.data.forEach(function(result) {
                        columns = result.columns;

                        if (arrayIDs.length != 0) {
                            for (i = 0; i < arrayIDs.length; i++) {
                                if (result.getValue(columns[8]) == arrayIDs[i]) {

                                    if (result.getValue(columns[0]) != '') {
                                        var keyNT = 'NT_' + result.getValue(columns[0]);
                                        if (jsonTemp[keyNT] == undefined) {
                                            jsonTemp[keyNT] = result.getValue(columns[1]) + '|' + result.getValue(columns[2]) + '|' + result.getValue(columns[6]) + '|' + result.getValue(columns[7]);
                                        } else {
                                            var tempJK = jsonTemp[keyNT].split('|');
                                            var tempMB = Number(tempJK[2]) + Number(result.getValue(columns[6]));
                                            var tempRT = Number(tempJK[3]) + Number(result.getValue(columns[7]));
                                            jsonTemp[keyNT] = tempJK[0] + '|' + tempJK[1] + '|' + tempMB + '|' + tempRT;
                                        }
                                    } else {
                                        var keyNT = 'CC_' + result.getValue(columns[3]);
                                        if (jsonTemp[keyNT] == undefined) {
                                            jsonTemp[keyNT] = result.getValue(columns[4]) + '|' + result.getValue(columns[5]) + '|' + result.getValue(columns[6]) + '|' + result.getValue(columns[7]);
                                        } else {
                                            var tempJK = jsonTemp[keyNT].split('|');
                                            var tempMB = Number(tempJK[2]) + Number(result.getValue(columns[6]));
                                            var tempRT = Number(tempJK[3]) + Number(result.getValue(columns[7]));
                                            jsonTemp[keyNT] = tempJK[0] + '|' + tempJK[1] + '|' + tempMB + '|' + tempRT;
                                        }
                                    }
                                }
                            }
                        }

                    });

                });

                log.debug('el jsontemp es: ', jsonTemp);
            }

            return jsonTemp;
        }

        function VerdaderoCodigoRJL(id_cuenta1, id_cuenta2) {
            var result = [0, 0];
            if (id_cuenta1 != '' && id_cuenta1 != 0 && id_cuenta2 != '' && id_cuenta2 != 0) {
                var accountSearchObj = search.create({
                    type: "account",
                    filters: [
                        ["internalid", "anyof", id_cuenta1, id_cuenta2],
                        'AND', ["formulatext: {custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_format_c.id}", "is", "2"]
                    ],
                    columns: [
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custrecord_lmry_co_puc_concept}",
                            label: "Formula (Text)"
                        }),
                        search.createColumn({
                            name: "custrecord_lmry_co_puc_format_c",
                            join: "custrecord_lmry_co_puc_concept"
                        })
                    ]
                });

                valor = accountSearchObj.run().getRange(0, 100);
                if (valor.length != 0) {
                    var columns = valor[0].columns;

                    valores = valor[0].getValue(columns[0]).split(",");
                    //log.error('valor columna 1 verdcod es: ', valor[0].getValue(columns[1]));
                    for (var i = 0; i < valores.length; i++) {
                        if (valor[0].getValue(columns[1]) == 2) {
                            if (valores[i].substring(0, 2) == '13') {
                                result[0] = valores[i].substring(0, 4);
                                result[1] = valor[0].getValue(columns[1]);
                                break;
                            }
                        }
                    }
                }
            }
            log.debug('el result vcRJL es: ', result);
            return result;
        }

        function ObtenerDatosTaxResultReclasification(id_transaccion_Ref, id_journal, type) {
            ObtenerParametrosYFeatures();
            var arrayIDs = [];

            var searchTaxResultReclasification = search.create({
                type: "customrecord_lmry_co_wht_reclasification",
                filters: [],
                columns: [
                    search.createColumn({
                        name: "custrecord_co_reclasification_return",
                    }),

                ]
            });

            if (featureSubsidiaria) {
                var subsidiaryFilter = search.createFilter({
                    name: 'custrecord_co_reclasification_subsi',
                    operator: search.Operator.IS,
                    values: [paramSubsidiaria]
                });
                searchTaxResultReclasification.filters.push(subsidiaryFilter);
            }

            if (type == 2) {

                if (paramPeriodo) {
                    var fechInicioFilter = search.createFilter({
                        name: 'custrecord_co_reclasification_condate',
                        operator: search.Operator.ONORAFTER,
                        values: [periodStartDate]
                    });
                    searchTaxResultReclasification.filters.push(fechInicioFilter);
                    var fechFinFilter = search.createFilter({
                        name: 'custrecord_co_reclasification_condate',
                        operator: search.Operator.ONORBEFORE,
                        values: [periodEndDate]
                    });
                    searchTaxResultReclasification.filters.push(fechFinFilter);
                }
            }

            var pagedData = searchTaxResultReclasification.runPaged({
                pageSize: 1000
            });

            var page, columns;

            pagedData.pageRanges.forEach(function(pageRange) {
                page = pagedData.fetch({
                    index: pageRange.index
                });

                page.data.forEach(function(result) {
                    columns = result.columns;
                    if (result.getValue(columns[0]) != '' && result.getValue(columns[0]) != null && result.getValue(columns[0]) != 0) {
                        var datosTemp = JSON.parse(result.getValue(columns[0]));
                        if (datosTemp[id_transaccion_Ref] != undefined) {
                            for (i = 0; i < datosTemp[id_transaccion_Ref].length; i++) {
                                if (type == 1) {
                                    arrayIDs.push(datosTemp[id_transaccion_Ref][i].taxResult);
                                } else {
                                    if (datosTemp[id_transaccion_Ref][i].retention == id_journal) {
                                        arrayIDs.push(datosTemp[id_transaccion_Ref][i].taxResult);
                                    }
                                }
                            }
                        }
                    }
                });

            });

            log.debug('el arrayIDs es: ', arrayIDs);

            return arrayIDs;
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
            reduce: reduce,
            map: map,
            summarize: summarize
        };
    });