/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||  This script for Report - Colombia                               ||
||                                                                  ||
||  File Name: LMRY_CO_CretificadoRetencion_SCHDL_V2.0.js           ||
||                                                                  ||
||  Version Date         Author        Remarks                      ||
||  2.0     FEB 20 2022  Alexandra      Use Script 2.0              ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */

/**
 * @NApiVersion 2.0
 * @NScriptType ScheduledScript
 * @NModuleScope Public
 */
define(['N/search',
        'N/suiteAppInfo',
        "N/format",
        'N/config',
        'N/record',
        'N/file',
        'N/log',
        'N/task',
        'N/runtime',
        './CO_Library/LMRY_CO_AccessData_LBRY_V2.0',
        './CO_Library/LMRY_CO_RenderTemplate_LBRY_V2.0',
        './CO_Library/LMRY_CO_Cache_LBRY_V2.0',
        "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js",
        "./CO_Library/LMRY_CO_Library_LBRY_V2.0.js"
    ],

    function(search, suiteAppInfo, format, config, record, file, log, task, runtime, AccessData, RenderTemplate, DataCache, libreriaReport, libColombia) {

        var objContext = runtime.getCurrentScript();
        // Parametros
        var language = '';
        var dataCache;
        var Inicio_Fecha;
        var Final_Fecha;
        var numMuni = 0;
        var municipality = 'Bogota';
        var extencion = 'pdf';
        var transacciones;
        var otherMunicipality;
        var jsonTransactionMunicip = {};

        var companyname;
        var companyruc;
        var companyaddress;

        var parametros;
        var featureSTXT = null;
        var cacheKeyName = "certificado_de_retencion";


        var LMRY_script = "LMRY - CO Certificado de Retencion SCHDL";
        var namereport = "RPT CO - Certificado de Retencion CO";

        function execute(context) {

            // try {

            obtenerParametros();
            ObtenerDatosSubsidiaria();

            var arrKeys = Object.keys(transacciones);
            if (arrKeys.length != 0) {
                //log.debug('JsonTransactions', JsonTransactions);
                jsonTransactionMunicip = obtenerTransaccionesXMunicipalidad(transacciones);

                for (key in jsonTransactionMunicip) {
                    log.debug('Muni: ' + key, jsonTransactionMunicip[key]);
                    municipality = key;
                    name_muni = key.split(' ').join('_');
                    getPDF(jsonTransactionMunicip[key]);
                }
            }
            /*} catch (error) {
                var varMsgError = 'Importante: Error al generar el reporte.';
                log.error('error', error);
                libreriaReport.CreacionFormError(namereport, LMRY_script, varMsgError, error);
            }*/

        };

        function obtenerParametros() {

            /*Bundle Suite Tax Engine*/
            var isBundleInstalled = suiteAppInfo.isBundleInstalled({
                bundleId: 237702
            });

            if (isBundleInstalled) {
                featureSTXT = true;
            } else {
                featureSTXT = false;
            }

            dataCache = DataCache.getCacheByKey(cacheKeyName)
            language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);

            if (!language) {
                language = 'en'
            }
            log.debug('dataCache', dataCache);

            //Obtener parametros
            parametros = dataCache.parametros;
            transacciones = dataCache.transaction;
            otherMunicipality = dataCache.muni_by_vendor_o_subsi;
            log.debug('parametros', parametros);

        }

        function agruparPorConcepto(transacciones) {

            var jsonAgrupado = {};

            for (key in transacciones) {
                var arr = transacciones[key]["transaction_json"];
                for (var i = 0; i < arr.length; i++) {
                    var concepto = arr[i]["description"];
                    var resultArray = [concepto, Number(arr[i]["lc_baseamount"]), Number(arr[i]["lc_whtamount"])]
                    if (jsonAgrupado[concepto] === undefined) {
                        jsonAgrupado[concepto] = resultArray;
                    } else {
                        jsonAgrupado[concepto][1] += Number(resultArray[1]);
                        jsonAgrupado[concepto][2] += Number(resultArray[2]);
                    }
                }
            }

            log.debug('jsonAgrupado', jsonAgrupado);
            return jsonAgrupado;
        }

        function obtenerTransaccionesXMunicipalidad(transacciones) {
            var jsonAgrupadoxMun = {};

            for (key in transacciones) {
                var muni_aux = transacciones[key]["municipality"];
                if (muni_aux != "") {
                    if (jsonAgrupadoxMun[muni_aux] == undefined) {
                        jsonAgrupadoxMun[muni_aux] = {}
                    }
                    if (jsonAgrupadoxMun[muni_aux][key] == undefined) {
                        jsonAgrupadoxMun[muni_aux][key] = {}
                    }
                    jsonAgrupadoxMun[muni_aux][key] = transacciones[key];
                } else {
                    if (jsonAgrupadoxMun[otherMunicipality] == undefined) {
                        jsonAgrupadoxMun[otherMunicipality] = {}
                    }
                    if (jsonAgrupadoxMun[otherMunicipality][key] == undefined) {
                        jsonAgrupadoxMun[otherMunicipality][key] = {}
                    }
                    jsonAgrupadoxMun[otherMunicipality][key] = transacciones[key];
                }
            }
            return jsonAgrupadoxMun;
        }

        function ObtenerDatosSubsidiaria() {

            log.debug('ENTRO', 'entro ObtenerDatosSubsidiaria');

            if (parametros.id_subsidiary != '') {

                companyname = ObtainNameSubsidiaria(parametros.id_subsidiary);
                companyruc = ObtainFederalIdSubsidiaria(parametros.id_subsidiary);
                companyaddress = ObtainAddressIdSubsidiaria(parametros.id_subsidiary);

            } else {

                var pageConfig = config.load({
                    type: config.Type.COMPANY_INFORMATION
                });
                companyruc = pageConfig.getValue('taxidnum');
                companyname = configpage.getValue('companyname');
                companyaddress = pageConfig.getFieldValue('address1');
            }
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
                log.error("Error en [ObtainNameSubsidiaria] ", err);
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
                log.error("Error en [ObtainFederalIdSubsidiaria] ", err);
            }
            return '';
        }

        function ObtainAddressIdSubsidiaria(subsidiary) {
            try {
                if (subsidiary != '' && subsidiary != null) {
                    var SubsidiAddress = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: subsidiary,
                        columns: ['address1']
                    });
                    return SubsidiAddress.address1

                }
            } catch (err) {
                log.error("Error en [ObtainAddressIdSubsidiaria] ", err);
            }
            return '';
        }

        function SaveFile(render_outpuFile) {
            var FolderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co_stx'
            });
            var nameFile = getNameFile(extencion);
            log.debug('nameFile', nameFile);
            log.debug('render', render_outpuFile);

            var periodName = libreriaReport.getAccountingPeriod(parametros.id_period, ['periodname']);
            periodName = periodName.periodname;
            var urlfile = libreriaReport.setFile(FolderId, render_outpuFile, nameFile, extencion);

            var usuarioTemp = runtime.getCurrentUser();
            var id = usuarioTemp.id;
            var employeename = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: id,
                columns: ['firstname', 'lastname']
            });
            var usuario = employeename.firstname + ' ' + employeename.lastname;

            log.debug('PARAM LOG', parametros.id_rpt_generator_log);
            if (numMuni > 0) {
                libColombia.create(periodName, namereport, parametros.id_subsidiary, parametros.id_multibook, usuario, urlfile, nameFile)
            } else {
                libColombia.load(parametros.id_rpt_generator_log, nameFile, urlfile)
            }
            numMuni++;
        }

        function getNameFile(id_extension) {
            log.debug('ENTRO AL NAME FILE');

            var name = '';

            name = 'COCertificado_' + companyruc + '_' + parametros.period_mes + '_' + parametros.period_anio + '_' + parametros.id_subsidiary + '_' + name_muni + '.' + id_extension;


            return name;
        }

        //-------------------------------------------------------------------------------------------------------
        //Obtiene Informacion Vendor: CompanyName / VatRegNumber
        //-------------------------------------------------------------------------------------------------------
        function ObtainVendor(idvendor) {
            try {
                if (featureSTXT) {

                    var intDMinReg = 0;
                    var intDMaxReg = 1000;
                    var DbolStop = false;
                    var arrAuxiliar;

                    var vendorSearchObj = search.create({
                        type: "vendor",
                        filters: [
                            ["internalid", "anyof", idvendor]
                        ],
                        columns: [
                            search.createColumn({ name: "companyname", label: "0. COMPANY NAME" }),
                            search.createColumn({
                                name: "taxregistrationnumber",
                                join: "taxRegistration",
                                label: "1. TAX REG NUMBER"
                            }),
                            search.createColumn({
                                name: "formulatext",
                                formula: "{isperson}",
                                label: "2. ISPERSON"
                            }),
                            search.createColumn({
                                name: "formulatext",
                                formula: "{custentity_lmry_digito_verificator}",
                                label: "3. DIG. VERIFICADOR"
                            }),
                            search.createColumn({ name: "firstname", label: "4. FIRST NAME" }),
                            search.createColumn({ name: "lastname", label: "5. LAST NAME" })
                        ]
                    });

                    var searchresult = vendorSearchObj.run();

                    while (!DbolStop) {
                        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                        if (objResult != null) {
                            var intLength = objResult.length;
                            if (intLength != 1000) {
                                DbolStop = true;
                            }

                            for (var i = 0; i < intLength; i++) {
                                var columns = objResult[i].columns;

                                arrAuxiliar = new Array();

                                //0. COMPANY NAME
                                if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                                    arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                                } else {
                                    arrAuxiliar[0] = '';
                                }

                                //1. TAX REG NUMBER
                                if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                                    arrAuxiliar[1] = objResult[i].getValue(columns[1]);
                                } else {
                                    arrAuxiliar[1] = '';
                                }

                                //2. ISPERSON
                                if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                                    arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                                } else {
                                    arrAuxiliar[2] = '';
                                }

                                //3. DIG. VERIFICADOR
                                if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                                    arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                                } else {
                                    arrAuxiliar[3] = '';
                                }

                                //4. FIRST NAME
                                if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                                    arrAuxiliar[4] = objResult[i].getValue(columns[4]);
                                } else {
                                    arrAuxiliar[4] = '';
                                }

                                //5. LAST NAME
                                if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                                    arrAuxiliar[5] = objResult[i].getValue(columns[5]);
                                } else {
                                    arrAuxiliar[5] = '';
                                }
                            }
                        } else {
                            DbolStop = true;
                        }
                    }

                    if (arrAuxiliar[2] != 'F') {
                        companyname_vendor = ValidarAcentos(arrAuxiliar[0]);

                    } else {
                        companyname_vendor = arrAuxiliar[4] + " " + arrAuxiliar[5];
                    }

                    if (arrAuxiliar[1] != '') {
                        nit_vendor = arrAuxiliar[1] + "-" + arrAuxiliar[3].substr(0, 1);
                    } else {
                        nit_vendor = "           ";
                    }

                } else {

                    if (idvendor != '' && idvendor != null) {

                        var columnFrom_temp = search.lookupFields({
                            type: search.Type.VENDOR,
                            id: idvendor,
                            columns: ['companyname', 'vatregnumber', 'custentity_lmry_digito_verificator', "isperson", "firstname", "lastname"]
                        });

                        if (columnFrom_temp.isperson) {
                            var columnFrom1 = columnFrom_temp.firstname + " " + columnFrom_temp.lastname;
                        } else {
                            var columnFrom1 = columnFrom_temp.companyname;
                        }
                        companyname_vendor = ValidarAcentos(columnFrom1);

                        if (columnFrom_temp.vatregnumber != null && columnFrom_temp.vatregnumber != "" && columnFrom_temp.vatregnumber != "- None -") {
                            if (columnFrom_temp.custentity_lmry_digito_verificator != null && columnFrom_temp.custentity_lmry_digito_verificator != "" && columnFrom_temp.custentity_lmry_digito_verificator != "- None -") {
                                var columnFrom2 = columnFrom_temp.vatregnumber;
                                var columnFrom3 = columnFrom_temp.custentity_lmry_digito_verificator;
                                nit_vendor = columnFrom2 + "-" + columnFrom3.substr(0, 1);
                            } else {
                                var columnFrom2 = columnFrom_temp.vatregnumber;
                                nit_vendor = columnFrom2 + "-" + " ";
                            }
                        } else {
                            nit_vendor = "           ";
                        }
                    }
                }
            } catch (err) {
                log.debug('error: ', err);
            }
            return true;
        }

        //-------------------------------------------------------------------------------------------------------
        //Generacion archivo PDF
        //-------------------------------------------------------------------------------------------------------
        function getPDF(JsonTransactions) {

            var montototalRet = 0;

            // Declaracion de variables
            var strName = '';
            var fecha_format = format.parse({
                value: parametros.id_Fecha_ini,
                type: format.Type.DATE
            });

            var MM = fecha_format.getMonth() + 1;
            var YYYY = fecha_format.getFullYear();
            var DD = fecha_format.getDate();

            if (('' + MM).length == 1) {
                MM = '0' + MM;
            }
            if (('' + DD).length == 1) {
                DD = '0' + DD;
            }
            Inicio_Fecha = DD + '/' + MM + '/' + YYYY;


            var fecha_format = format.parse({
                value: parametros.id_Fecha_fin,
                type: format.Type.DATE
            });

            var MM = fecha_format.getMonth() + 1;
            var YYYY = fecha_format.getFullYear();
            var DD = fecha_format.getDate();

            if (('' + MM).length == 1) {
                MM = '0' + MM;
            }
            if (('' + DD).length == 1) {
                DD = '0' + DD;
            }

            Final_Fecha = DD + '/' + MM + '/' + YYYY;

            var reteType = '';
            var strConcepto = '';
            var tipoRetencion = String(parametros.id_Tipo_de_retencion);

            switch (tipoRetencion) {
                case '1':
                    reteType = 'ReteICA';
                    strConcepto = 'Retencion ICA';
                    break;
                case '2':
                    reteType = 'ReteFte';
                    strConcepto = 'Retencion FTE';
                    break;
                case '3':
                    reteType = 'ReteIVA';
                    strConcepto = 'Retencion IVA';
                    break;
                default:
                    log.debug('El tipo de retencion no coincide con ICA, IVA o FTE');
            }

            var GLOBAL_LABELS = {

                'es': {
                    'titulo': 'CERTIFICADO DE RETENCION',
                    'text1': 'Para dar Cumplimiento al articulo 381 de Estatuto Tributario, certificamos que durante el periodo comprendido entre el' + Inicio_Fecha + " y el " + Final_Fecha + ' , practicamos retenciones a titulo de ' + reteType,
                    'namedian': "DIRECCION DE IMPUESTOS Y ADUANAS NACIONALES DIAN",
                    'text2': "Los valores retenidos fueron consignados oportunamente a favor de la dianName en la Ciudad de " + municipality + ".",
                    'cabecera': {
                        'concepto': 'CONCEPTO',
                        'factura': 'N. FACTURA',
                        'baseret': ['BASE', 'RETENCION'],
                        'porcentaje': 'PORC.',
                        'valorret': ['VALOR', 'RETENIDO']
                    },
                    'firma': "SE EXPIDE SIN FIRMA AUTOGRAFA",
                    'domicilio': "DOMICILIO PRINCIPAL: ",
                    'fecha': "FECHA DE EXPEDICION: "
                },
                'en': {
                    'titulo': 'WITHHOLDING CERTIFICATE',
                    'text1': 'To comply with article 381 of the Tax Statute, we certify that during the period from ' + Inicio_Fecha + " to " + Final_Fecha + ' , we make retentions as ' + reteType,
                    'namedian': "DIRECTORATE OF TAXES AND NATIONAL CUSTOMS DIAN",
                    'text2': "The amounts withheld were duly consigned in favor of dianName in the City of " + municipality + ".",
                    'cabecera': {
                        'concepto': 'CONCEPT',
                        'factura': 'N. BILL',
                        'baseret': ['RETENTION', 'BASE'],
                        'porcentaje': 'PERC.',
                        'valorret': ['WITHHOLDING', 'VALUE']
                    },
                    'firma': "ISSUED WITHOUT AUTOGRAPH SIGNATURE",
                    'domicilio': "PRIMARY RESIDENCE: ",
                    'fecha': "EXPEDITION DATE: "
                },
                'pt': {
                    'titulo': 'CERTIFICADO DE RETENCAO',
                    'text1': 'Para cumprimento do artigo 381 do Regime Tributario, certificamos que durante o periodo comprendido entre o' + Inicio_Fecha + " e o " + Final_Fecha + ' , fazemos retencoes como ' + reteType,
                    'namedian': "DIRECCAO TRIBUTARIA E DIA ADUANEIRA NACIONAL",
                    'text2': "Os valores retidos foram devidamente consignados em favor de dianName na cidade de " + municipality + ".",
                    'cabecera': {
                        'concepto': 'CONCEITO',
                        'factura': 'N. FATURA',
                        'baseret': ['BASE', 'RETENCAO'],
                        'porcentaje': 'PERC.',
                        'valorret': ['RETENCAO', 'DE VALOR']
                    },
                    'firma': "EMITIDO SEM ASSINATURA DE AUTOGRAFO",
                    'domicilio': "RESIDENCIA PRIMARIA: ",
                    'fecha': "DATA DE EXPEDICAO: "
                }

            }

            if (reteType == 'ReteICA') {
                var vendorName = parametros.namedian;
            } else {
                var vendorName = GLOBAL_LABELS[language]['namedian'];
            }

            ObtainVendor(parametros.id_Vendor);

            //-------------------------------------------------------------------------------------------------------
            //Cabecera del reporte
            //-------------------------------------------------------------------------------------------------------
            var strHead = '';
            strHead += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\" align=\"center\">";
            strHead += "<p>" + ValidarAcentos(companyname) + "</p>";
            strHead += "</td>";
            strHead += "</tr>";
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\" align=\"center\">";
            strHead += companyruc;
            strHead += "</td>";
            strHead += "</tr>";
            strHead += "</table>";
            strHead += "<p></p>";

            strHead += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 16pt; border: 0px solid #000000\" align=\"center\">";
            strHead += "<p>" + GLOBAL_LABELS[language]['titulo'] + "</p>";
            strHead += "</td>";
            strHead += "</tr>";
            // Impuesto

            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += GLOBAL_LABELS[language]['text1'];
            strHead += "</td>";
            strHead += "</tr>";

            strHead += "</table>";

            strHead += "<p></p>";
            strHead += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + companyname_vendor + "</p>";
            strHead += "</td>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + nit_vendor + "</p>";
            strHead += "</td>";
            strHead += "</tr>";

            strHead += "</table>";
            strHead += "<p></p>";

            strName += strHead;

            //-------------------------------------------------------------------------------------------------------
            //Detalle del reporte
            //-------------------------------------------------------------------------------------------------------
            var strDeta = '';
            strDeta += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strDeta += "<tr>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"25mm\">";
            strDeta += "<p>" + GLOBAL_LABELS[language]['cabecera']['concepto'] + "</p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"65mm\">";
            strDeta += "<p>" + GLOBAL_LABELS[language]['cabecera']['factura'] + "</p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
            strDeta += GLOBAL_LABELS[language]['cabecera']['baseret'][0];
            strDeta += "<br/>";
            strDeta += GLOBAL_LABELS[language]['cabecera']['baseret'][1];
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"30mm\">";
            strDeta += "<p>" + GLOBAL_LABELS[language]['cabecera']['porcentaje'] + "</p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
            strDeta += GLOBAL_LABELS[language]['cabecera']['valorret'][0];
            strDeta += "<br/>";
            strDeta += GLOBAL_LABELS[language]['cabecera']['valorret'][1];
            strDeta += "</td>";
            strDeta += "</tr>";

            for (idTransaction in JsonTransactions) {

                //NO. FACTURA
                var noFactura = JsonTransactions[idTransaction].transaction_no_factura;
                var arrRetencion = JsonTransactions[idTransaction].transaction_json;

                for (var i = 0; i < arrRetencion.length; i++) {

                    montototalRet = parseFloat(montototalRet) + parseFloat(Math.round(parseFloat(Number(arrRetencion[i]['lc_whtamount'])) * 100) / 100);

                    strDeta += "<tr>";
                    strDeta += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\">";
                    strDeta += "<p>" + arrRetencion[i]['description'] + "</p>";
                    strDeta += "</td>";
                    strDeta += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\">";
                    strDeta += "<p>" + noFactura + "</p>";
                    strDeta += "</td>";
                    strDeta += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                    strDeta += "<p>" + FormatoNumero(parseFloat(arrRetencion[i]['lc_baseamount']).toFixed(2), "$") + "</p>";
                    strDeta += "</td>";
                    strDeta += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                    strDeta += "<p>" + arrRetencion[i]['whtrate'] + "</p>";
                    strDeta += "</td>";
                    strDeta += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                    strDeta += "<p>" + FormatoNumero(parseFloat(arrRetencion[i]['lc_whtamount']).toFixed(2), "$") + "</p>";
                    strDeta += "</td>";
                    strDeta += "</tr>";

                }
            }

            strDeta += "<tr>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"50mm\">";
            strDeta += "<p>TOTAL</p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"35mm\">";
            strDeta += "<p></p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"45mm\">";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"22mm\">";
            strDeta += "<p></p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"right\" width=\"42mm\">";
            strDeta += "<p>" + FormatoNumero(parseFloat(montototalRet).toFixed(2), "$") + "</p>";
            strDeta += "</td>";
            strDeta += "</tr>";

            // cierra la tabla
            strDeta += "</table>";

            strName += strDeta;

            var strNpie = '';

            strNpie += "<p></p>";
            strNpie += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strNpie += GLOBAL_LABELS[language]['text2'].replace('dianName', vendorName);
            strNpie += "</td>";
            strNpie += "</tr>";
            strNpie += "</table>";

            strNpie += "<p></p>";
            strNpie += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strNpie += GLOBAL_LABELS[language]['firma'];
            strNpie += "</td>";
            strNpie += "</tr>";
            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strNpie += "(ART .10 D.R. 836/91)";
            strNpie += "</td>";
            strNpie += "</tr>";

            var fecha_actual = new Date();
            fecha_actual = fecha_actual.getDate() + "/" + (fecha_actual.getMonth() + 1) + "/" + fecha_actual.getFullYear();

            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strNpie += GLOBAL_LABELS[language]['domicilio'] + companyaddress;
            strNpie += "</td>";
            strNpie += "</tr>";
            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strNpie += GLOBAL_LABELS[language]['fecha'] + fecha_actual;
            strNpie += "</td>";
            strNpie += "</tr>";
            strNpie += "</table>";
            strNpie += "<p></p>";

            strName += strNpie;

            Final_string = "<?xml version=\"1.0\"?>\n<!DOCTYPE pdf PUBLIC \"-//big.faceless.org//report\" \"report-1.1.dtd\">\n";
            Final_string += '<pdf><head><style> body {size:A4}</style></head><body>';
            Final_string += strName;
            Final_string += "</body>\n</pdf>";

            SaveFile(Final_string);
        }

        function ValidarAcentos(s) {
            var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ&°–—ªº·";
            var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyyo--ao.";

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

        //-------------------------------------------------------------------------------------------------------
        //Formato de Numero con miles-decimales
        //-------------------------------------------------------------------------------------------------------
        function FormatoNumero(pNumero, pSimbolo) {
            var separador = ',';
            var sepDecimal = '.';

            var splitStr = pNumero.split('.');
            var splitLeft = splitStr[0];
            var splitRight = splitStr.length > 1 ? sepDecimal + splitStr[1] : '';
            var regx = /(\d+)(\d{3})/;
            while (regx.test(splitLeft)) {
                splitLeft = splitLeft.replace(regx, '$1' + separador + '$2');
            }
            pSimbolo = pSimbolo || '';
            var valor = pSimbolo + splitLeft + splitRight;
            return valor;
        }

        return {
            execute: execute
        };
    });