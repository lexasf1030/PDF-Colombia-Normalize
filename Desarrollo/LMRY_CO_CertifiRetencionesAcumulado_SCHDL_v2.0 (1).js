/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_CertifiRetencionesAcumulado_SCHDL_v2.0.js||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0    OCTUBRE 23 2018  LatamReady    Use Script 2.0        ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType ScheduledScript
 * @NModuleScope Public
 */
define(["N/record", "N/runtime", "N/file", "N/email", "N/encode", "N/search",
        "N/format", "N/log", "N/config", "N/sftp", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js", "N/task", "N/render", "N/url", "N/xml",
        "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"
    ],

    function(recordModulo, runtime, fileModulo, email, encode, search, format, log,
        config, sftp, libreria, task, render, url, xml, library) {
        var objContext = runtime.getCurrentScript();

        var anio = '';
        //Tamaño
        var file_size = 7340032;

        // Nombre del Reporte
        var namereport = "Reporte de Certificado de Retenciones Acumulado";
        var LMRY_script = 'LMRY CO Reportes Certificado de Retencion Acumulado SCHDL 2.0';

        //Parametros
        var paramsubsidi = '';
        var paramVendor = '';
        var paramTyreten = '';
        var paramidrpt = '';
        var paramMulti = '';
        var paramCont = '';
        var paramBucle = '';
        var paramGroupingMonth = '';
        var paramperiodanio = '';
        var jsonNameMonth = {};

        //Control de Reporte
        var periodstartdate = '';
        var periodenddate = '';
        var antperiodenddate = '';
        var companyruc = '';
        var companyname = '';
        var companyaddress = '';
        var companyname_vendor = '';
        var nit_vendor = '';
        var companycity = '';

        var fechalog = '';
        var ArrReteAux = new Array();
        var ArrRetencion = new Array();
        var ArrRetencion2 = new Array();
        var formulPeriodFilters = new Array();

        var result_f;

        var result_wht_code;
        var searchresultWhtCode_fin;

        var strName = '';
        var strNameFile = 'CRA';
        var periodname = '';
        var auxmess = '';
        var auxanio = '';
        var Final_string;
        var columnas_f = new Array();
        var ExchangerateC_S;
        var Inicio = '';
        var Final = '';
        var Inicio_Fecha = '';
        var Final_Fecha = '';
        var periodFilters = '';

        var multibook = '';

        // Para el logo de la subsidiaria
        var subsi_logo = ''



        /******************************************
         * @leny - Modificado el 28/08/2015
         * Nota: Variables para acumulacions de Montos.
         ******************************************/
        var montototal = 0;
        var montoBase = 0;
        var nameDIAN = '';
        var municipality = '';
        var name_muni = '';
        var num_muni = 0;

        /* ***********************************************
         * Arreglo con la structura de la tabla log
         * ******************************************** */
        var RecordName = 'customrecord_lmry_co_rpt_generator_log';
        var RecordTable = ['custrecord_lmry_co_rg_name',
            'custrecord_lmry_co_rg_postingperiod',
            'custrecord_lmry_co_rg_subsidiary',
            'custrecord_lmry_co_rg_url_file',
            'custrecord_lmry_co_rg_employee',
            'custrecord_lmry_co_rg_multibook'
        ];

        //Features
        var featSubsi = null;
        var featMulti = null;

        //var featuremultib = objContext.getFeature('MULTIBOOKMULTICURR');

        var result_f;

        var GLOBAL_LABELS = {};
        var language = runtime.getCurrentScript().getParameter({
            name: 'LANGUAGE'
        }).substring(0, 2);

        //PDF Normalization
        var todays = "";
        var currentTime = "";

        function execute(context) {

            //try {
            GLOBAL_LABELS = getGlobalLabels();
            language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);

            ObtenerParametrosYFeatures();

            ObtenerDatosSubsidiaria();

            var arregloidPeriod = getPeriods(paramperiodanio, paramsubsidi) || [];

            log.debug('accounting arrperiod', arregloidPeriod);

            formulPeriodFilters = generarStringFilterPostingPeriodAnual(arregloidPeriod);
            log.debug('formulPeriodFilters', formulPeriodFilters);

            jsonNameMonth = obtenerMeses();

            // Retencion ICA
            if (paramTyreten == 1) {
                ArrRetencion = ObtieneRetencionReteICA();
            }
            // Retencion en la Fuente
            if (paramTyreten == 2) {
                ArrRetencion = ObtieneRetencionReteFTE();
            }
            // Retencion IVA
            if (paramTyreten == 3) {
                ArrRetencion = ObtieneRetencionReteIVA();
            }

            todays = parseDateTo(new Date(), "DATE");
            currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));
            log.debug('ArrRetencion', ArrRetencion);

            if (ArrRetencion.length != 0) {
                if (paramTyreten == 1) {

                    jsonTransactionMunicip = obtenerTransaccionesXMunicipalidad(ArrRetencion);

                    for (key in jsonTransactionMunicip) {
                        //log.debug('Muni: ' + key, jsonTransactionMunicip[key]);
                        municipality = key;
                        name_muni = key.split(' ').join('_');
                        ArrRetencion = jsonTransactionMunicip[key];
                        GeneracionPDF();
                    }
                } else {
                    name_muni = municipality.split(' ').join('_');
                    GeneracionPDF();
                }
            } else {
                RecordNoData();
                return false;
            }


            /*} catch (err) {
                libreria.sendMail(LMRY_script, ' [ execute ] ' + err);
                //var varMsgError = 'No se pudo procesar el Schedule.';
            }*/
        }

        //AGRUPA LAS TRANSACCIONES POR MUNICIPALIDAD
        function obtenerTransaccionesXMunicipalidad(ArrRetencion) {
            var jsonAgrupadoxMun = {};
            for (var i = 0; i < ArrRetencion.length; i++) {

                var municipalidad = (getNameSubsidiaria(ArrRetencion[i][5]) || municipality);

                if (jsonAgrupadoxMun[municipalidad] != undefined) {
                    jsonAgrupadoxMun[municipalidad].push(ArrRetencion[i]);
                } else {
                    jsonAgrupadoxMun[municipalidad] = [ArrRetencion[i]]
                }
            }
            return jsonAgrupadoxMun;
        }

        // obtiene id de periodos menos quarter y year
        function ObtieneWhtCode() {
            // Control de Memoria
            var intDMaxReg = 1000;
            var intDMinReg = 0;
            var searchresult_WhtCode = new Array();
            // Exedio las unidades
            var DbolStop = false;
            var _contPer = 0;

            var SearchWhtCode = search.create({
                type: 'customrecord_lmry_wht_code',

                columns: [
                    search.createColumn({
                        name: 'name'
                    }),
                    search.createColumn({
                        name: "custrecord_lmry_wht_codedesc",
                        label: "Name"
                    }),
                    search.createColumn({
                        name: "custrecord_lmry_wht_coderate",
                        label: "Name"
                    })

                ]
            });

            var searchresult_WhtCode = SearchWhtCode.run();

            while (!DbolStop) {
                var objResult = searchresult_WhtCode.getRange(intDMinReg, intDMaxReg);
                if (objResult != null) {
                    var intLength = objResult.length;

                    if (intLength != 1000) {
                        DbolStop = true;
                    }
                    for (var i = 0; i < intLength; i++) {
                        var columns = objResult[i].columns;

                        arrAuxiliarWhtCode = new Array();

                        //0. name
                        if (objResult[i].getValue(columns[0]) != null)
                            arrAuxiliarWhtCode[0] = objResult[i].getValue(columns[0]);
                        else
                            arrAuxiliarWhtCode[0] = '';

                        //1. Code desc
                        if (objResult[i].getValue(columns[1]) != null)
                            arrAuxiliarWhtCode[1] = objResult[i].getValue(columns[1]);
                        else
                            arrAuxiliarWhtCode[1] = '';

                        //2. Code Rate
                        if (objResult[i].getValue(columns[2]) != null)
                            arrAuxiliarWhtCode[2] = objResult[i].getValue(columns[2]);
                        else
                            arrAuxiliarWhtCode[2] = '';


                        ArrWhtCode[_contPer] = arrAuxiliarWhtCode;
                        _contPer++;

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
            if (splitLeft.charAt(0) === '-') {
                splitLeft = splitLeft.slice(1)
                pSimbolo = '-' + pSimbolo
            }
            var valor = pSimbolo + splitLeft + splitRight;
            return valor;
        }

        //-------------------------------------------------------------------------------------------------------
        //Generaci?n Detalle Retencion en PDF
        //-------------------------------------------------------------------------------------------------------
        function DetalleRetencionxMes(ArrAcumulado) {
            var strAux = '';

            for (var i = 0; i <= ArrAcumulado.length - 1; i++) {
                /******************************************
                 * @leny - Modificado el 28/08/2015
                 * Nota: Se acumulacion de montos.
                 ******************************************/
                //montoBase = parseFloat(montoBase) + parseFloat(ArrAcumulado[i][2]);

                //montototal = parseFloat(montototal) + parseFloat(ArrRetencion[i][7]);
                //montototal = parseFloat(montototal) + parseFloat(Math.round(parseFloat(Number(ArrAcumulado[i][7])) * 100) / 100);
                if (ArrAcumulado[i][2].slice(-1) == "%") {
                    ArrAcumulado[i][2] = ArrAcumulado[i][2].substr(0, ArrAcumulado[i][2].length - 1);
                }

                strAux += "<tr>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\">";
                strAux += "<p>" + ArrAcumulado[i][4] + "</p>";
                strAux += "</td>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\">";
                strAux += "<p>" + xml.escape(ArrAcumulado[i][0]) + "</p>";
                strAux += "</td>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                strAux += "<p>" + FormatoNumero(parseFloat(ArrAcumulado[i][1]).toFixed(2), "$") + "</p>";
                strAux += "</td>";
                //  strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                //  strAux += "<p>" + ArrAcumulado[i][2] + "%</p>";
                //  strAux += "</td>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                strAux += "<p>" + FormatoNumero(parseFloat(ArrAcumulado[i][3]).toFixed(2), "$") + "</p>";
                strAux += "</td>";
                strAux += "</tr>";
                log.error('ArrAcumulado string', ArrAcumulado[i][0] + ' - ' + FormatoNumero(parseFloat(ArrAcumulado[i][1]).toFixed(2), "$") + ' - ' + ArrAcumulado[i][2] + ' - ' + FormatoNumero(parseFloat(ArrAcumulado[i][3]).toFixed(2), "$"));
            }

            return strAux;
        }

        function DetalleRetencion(ArrAcumulado) {
            var strAux = '';

            for (var i = 0; i <= ArrAcumulado.length - 1; i++) {
                /******************************************
                 * @leny - Modificado el 28/08/2015
                 * Nota: Se acumulacion de montos.
                 ******************************************/
                //montoBase = parseFloat(montoBase) + parseFloat(ArrAcumulado[i][2]);

                //montototal = parseFloat(montototal) + parseFloat(ArrRetencion[i][7]);
                //montototal = parseFloat(montototal) + parseFloat(Math.round(parseFloat(Number(ArrAcumulado[i][7])) * 100) / 100);
                if (ArrAcumulado[i][2].slice(-1) == "%") {
                    ArrAcumulado[i][2] = ArrAcumulado[i][2].substr(0, ArrAcumulado[i][2].length - 1);
                }

                strAux += "<tr>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\">";
                strAux += "<p>" + xml.escape(ArrAcumulado[i][0]) + "</p>";
                strAux += "</td>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                strAux += "<p>" + FormatoNumero(parseFloat(ArrAcumulado[i][1]).toFixed(2), "$") + "</p>";
                strAux += "</td>";
                //  strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                //  strAux += "<p>" + ArrAcumulado[i][2] + "%</p>";
                //  strAux += "</td>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                strAux += "<p>" + FormatoNumero(parseFloat(ArrAcumulado[i][3]).toFixed(2), "$") + "</p>";
                strAux += "</td>";
                strAux += "</tr>";
                log.error('ArrAcumulado string', ArrAcumulado[i][0] + ' - ' + FormatoNumero(parseFloat(ArrAcumulado[i][1]).toFixed(2), "$") + ' - ' + ArrAcumulado[i][2] + ' - ' + FormatoNumero(parseFloat(ArrAcumulado[i][3]).toFixed(2), "$"));
            }

            return strAux;
        }



        function Name_File() {
            log.error('ENTRo name_file', 'Name_File');
            //AR_RETE_PERC_IIBB_CABA_XXXXXX_MMYYYY_S_T_M_C.txt
            var _NameFile = '';
            var vatRegNumber = getVatRegistrationNo(paramsubsidi);
            _NameFile = strNameFile + '_' + vatRegNumber + '_' + anio + '_' + paramsubsidi;
            return _NameFile;

        }


        function calcularAnio() {

            //Mostrar año
            var licenses = library.getLicenses(paramsubsidi);

            featureSpecialPeriod = library.getAuthorization(677, licenses);

            if (featureSpecialPeriod == true || featureSpecialPeriod == 'T') {
                anio = paramperiodanio;
            } else {
                var periodAnioSearchObj = search.lookupFields({
                    type: search.Type.ACCOUNTING_PERIOD,
                    id: paramperiodanio,
                    columns: ['startdate', 'enddate']
                });
                var periodStartDate = periodAnioSearchObj.startdate;
                var fecha_format = format.parse({
                    value: periodStartDate,
                    type: format.Type.DATE
                });
                anio = fecha_format.getFullYear();
                //log.debug('anio', anio);
            }
        }

        //-------------------------------------------------------------------------------------------------------
        // Graba el archivo en el Gabinete de Archivos
        //-------------------------------------------------------------------------------------------------------
        function SaveFile() {
            log.error('ENTRO1', 'entro SaveFile');

            var objContext = runtime.getCurrentScript();
            // Ruta de la carpeta contenedora
            var FolderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co'
            });

            // Almacena en la carpeta de Archivos Generados
            if (FolderId != '' && FolderId != null) {
                // Genera el nombre del archivo
                var fileext;
                var NameFile;

                // Crea el archivo
                fileext = '.pdf';
                if (featMulti) {
                    if (Number(paramCont) == 0) {
                        NameFile = Name_File() + '_' + name_muni + '_' + paramMulti + fileext;
                    } else {
                        NameFile = Name_File() + '_' + name_muni + '_' + paramMulti + '_' + paramCont + fileext;
                    }
                } else {
                    if (Number(paramCont) == 0) {
                        NameFile = Name_File() + '_' + name_muni + fileext;
                    } else {
                        NameFile = Name_File() + '_' + name_muni + '_' + paramCont + fileext;
                    }
                }

                //  log.debug('final string', Final_string);

                // Crea el PDF
                var file = render.xmlToPdf(Final_string);


                file.name = NameFile;
                file.folder = FolderId;

                // Termina de grabar el archivo
                var idfile = file.save();

                // Trae URL de archivo generado
                var idfile2 = fileModulo.load({
                    id: idfile
                });

                // Obtenemos de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
                var getURL = objContext.getParameter({
                    name: 'custscript_lmry_netsuite_location'
                });

                var urlfile = '';

                log.error('getURL', getURL);

                if (getURL != '' && getURL != '') {
                    urlfile += 'https://' + getURL;
                }
                urlfile += idfile2.url;

                log.error('urlfile', urlfile);

                //Genera registro personalizado como log
                if (idfile) {

                    var usuarioTemp = runtime.getCurrentUser();
                    var id = usuarioTemp.id;
                    var employeename = search.lookupFields({
                        type: search.Type.EMPLOYEE,
                        id: id,
                        columns: ['firstname', 'lastname']
                    });
                    var usuario = employeename.firstname + ' ' + employeename.lastname;
                    if (Number(paramCont) > 1 || num_muni > 0) {
                        var record = recordModulo.create({
                            type: 'customrecord_lmry_co_rpt_generator_log',

                        });

                        //Nombre de Archivo
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_name',
                            value: NameFile
                        });
                        log.error('NameFile', NameFile);

                        //Periodo
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_postingperiod',
                            value: anio
                        });

                        //Nombre de Reporte
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_transaction',
                            value: 'CO - Certificado de Retención Acumulada'
                        });

                        //Nombre de Subsidiaria
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_subsidiary',
                            value: companyname
                        });


                        //Url de Archivo
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_url_file',
                            value: urlfile
                        });


                        //Multibook
                        if (featMulti || featMulti == 'T') {
                            record.setValue({
                                fieldId: 'custrecord_lmry_co_rg_multibook',
                                value: multibookName
                            });
                        }

                        //Creado Por
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_employee',
                            value: usuario
                        });

                        var recordId = record.save();

                        // Envia mail de conformidad al usuario
                        libreria.sendrptuser('CO - Certificado de Retención Acumulada', 3, NameFile);
                    } else {
                        var record = recordModulo.load({
                            type: 'customrecord_lmry_co_rpt_generator_log',
                            id: paramidrpt
                        });

                        //Nombre de Archivo
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_name',
                            value: NameFile
                        });
                        log.error('NameFile', NameFile);


                        //Url de Archivo
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_url_file',
                            value: urlfile
                        });

                        var recordId = record.save();

                        // Envia mail de conformidad al usuario
                        libreria.sendrptuser('CO - Certificado de Retención Acumulada', 3, NameFile);
                    }
                }
                num_muni++;
            } else {
                // Debug
                log.error({
                    title: 'DEBUG',
                    details: 'Creacion de Txt' +
                        'No se existe el folder'
                });
            }
        }

        function RecordNoData() {
            log.error('ENTRO', 'entro RecordNoData');

            var usuarioTemp = runtime.getCurrentUser();

            var id = usuarioTemp.id;
            var employeename = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: id,
                columns: ['firstname', 'lastname']
            });
            var usuario = employeename.firstname + ' ' + employeename.lastname;

            var record = recordModulo.load({
                type: 'customrecord_lmry_co_rpt_generator_log',
                id: paramidrpt
            });

            //Nombre de Archivo
            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: 'No existe informacion para los criterios seleccionados.'
            });

            var recordId = record.save();
        }

        //-------------------------------------------------------------------------------------------------------
        //Obtiene Informacion Vendor: CompanyName / VatRegNumber
        //-------------------------------------------------------------------------------------------------------
        function ObtainVendor(idvendor) {
            try {
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
                            nit_vendor = columnFrom2;
                        }
                    } else {
                        nit_vendor = "           ";
                    }
                }
            } catch (err) {
                sendemail(' [ ObtainVendor ] ' + err, LMRY_script);
            }
            return true;
        }

        //-------------------------------------------------------------------------------------------------------
        //Generaci?n archivo PDF
        //-------------------------------------------------------------------------------------------------------
        function GeneracionPDF() {

            var noDataFound = true;
            montototal = 0;
            var title = getTitle(paramTyreten);

            // Declaracion de variables
            var strName = '';

            var anio_gravable = anio;

            //  Para obtener el NIT o Cedula
            var nitCedula = getVatRegistrationNo(paramsubsidi);


            subsi_logo = logoSubsidiary(paramsubsidi);
            //  log.error('subsi_logo',subsi_logo);

            subsi_logo = xml.escape(subsi_logo);

            ObtainVendor(paramVendor);

            //-------------------------------------------------------------------------------------------------------
            //Cabecera del reporte
            //-------------------------------------------------------------------------------------------------------
            var strHead = '';
            //  Para el logo
            strHead += "<div style=\"font-size: 10px; width:100%\">";
            strHead += "<img align=\"right\" src='" + subsi_logo + "' alt=\"logo\" height=\"50px\"></img>";
            strHead += "</div>";

            //  Head
            strHead += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 16pt; border: 0px solid #000000\" align=\"center\">";
            strHead += "<p>" + title + "</p>";
            strHead += "</td>";
            strHead += "</tr>";
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 14pt; border: 0px solid #000000\" align=\"center\">";
            strHead += "<p>" + GLOBAL_LABELS['AnioGravable'][language] + anio_gravable + "</p>";
            strHead += "</td>";
            strHead += "</tr>";

            // Impuesto ICA
            if (paramTyreten == 1) {
                strHead += "<tr>";
                strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
                strHead += GLOBAL_LABELS['Articulo381_ICA'][language];
                strHead += "</td>";
                strHead += "</tr>";
            }
            // Impuesto RENTA
            if (paramTyreten == 2) {
                strHead += "<tr>";
                strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
                strHead += GLOBAL_LABELS['Articulo381_RENTA'][language];
                strHead += "</td>";
                strHead += "</tr>";
            }
            // Impuesto IVA
            if (paramTyreten == 3) {
                strHead += "<tr>";
                strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
                strHead += GLOBAL_LABELS['Articulo381_IVA'][language];
                strHead += "</td>";
                strHead += "</tr>";
            }
            strHead += "</table>";

            strHead += "<p></p>";


            strHead += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strHead += "<tr>";
            //  Agente Retenedor
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p><b>" + GLOBAL_LABELS['AgenteRetenedor'][language] + "</b></p>";
            strHead += "</td>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + xml.escape(ValidarAcentos(companyname)) + "</p>";
            strHead += "</td>";
            strHead += "<td></td>";
            strHead += "<td></td>";
            strHead += "<td></td>";
            strHead += "</tr>";

            //  NIT o Cédula
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p><b>" + GLOBAL_LABELS['NITCédula'][language] + "</b></p>";
            strHead += "</td>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + nitCedula + "</p>";
            strHead += "</td>";
            strHead += "<td>";
            strHead += "</td>";
            strHead += "</tr>";

            //  Dirección
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p><b>" + GLOBAL_LABELS['Direccion'][language] + "</b></p>";
            strHead += "</td>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + ValidarAcentos(xml.escape(companyaddress)) + "</p>";
            strHead += "</td>";
            strHead += "</tr>";

            //  Ciudad
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p><b>" + GLOBAL_LABELS['Ciudad'][language] + "</b></p>";
            strHead += "</td>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + companycity + "</p>";
            strHead += "</td>";
            strHead += "</tr>";

            strNpie += "<p></p>";

            //  Pagado a
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p><b>" + GLOBAL_LABELS['PagadoA'][language] + "</b></p>";
            strHead += "</td>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + xml.escape(companyname_vendor) + "</p>";
            strHead += "</td>";
            strHead += "</tr>";

            //  NIT/ Cédula
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p><b>" + GLOBAL_LABELS['NIT/Cédula'][language] + "</b></p>";
            strHead += "</td>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + nit_vendor + "</p>";
            strHead += "</td>";
            strHead += "</tr>";

            //  Origin
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p><b>" + GLOBAL_LABELS['origin'][language] + "</b></p>";
            strHead += "</td>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + "Netsuite" + "</p>";
            strHead += "</td>";
            strHead += "</tr>";

            //  Date
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p><b>" + GLOBAL_LABELS['date'][language] + "</b></p>";
            strHead += "</td>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + todays + "</p>";
            strHead += "</td>";
            strHead += "</tr>";

            //  Time
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p><b>" + GLOBAL_LABELS['time'][language] + "</b></p>";
            strHead += "</td>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + currentTime + "</p>";
            strHead += "</td>";
            strHead += "</tr>";

            //  strHead += "<tr>";
            //  strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            //  strHead += "<p>" + nit_vendor + "</p>";
            //  strHead += "</td>";
            //  strHead += "</tr>";


            strHead += "</table>";
            strHead += "<p></p>";

            strName += strHead;


            //-------------------------------------------------------------------------------------------------------
            //Detalle del reporte
            //-------------------------------------------------------------------------------------------------------

            //TODO: CREAR EL PARAMETRO QUE VENGA DESDE EL SUITELET
            if (paramGroupingMonth == true || paramGroupingMonth == 'T') {
                var strDeta = '';
                strDeta += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
                strDeta += "<thead>";
                strDeta += "<tr>";
                strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"25mm\">";
                strDeta += "<p>" + GLOBAL_LABELS['MES'][language] + "</p>";
                strDeta += "</td>";
                strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"25mm\">";
                strDeta += "<p>" + GLOBAL_LABELS['CONCEPTO'][language] + "</p>";
                strDeta += "</td>";
                strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
                strDeta += GLOBAL_LABELS['BASE'][language] + "<br/>";
                strDeta += GLOBAL_LABELS['RETENCION'][language];
                strDeta += "</td>";
                //  strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"30mm\">";
                //  strDeta += "<p>"+GLOBAL_LABELS['PORC'][language]+"</p>";
                //  strDeta += "</td>";
                strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
                strDeta += GLOBAL_LABELS['VALOR'][language] + "<br/>";
                strDeta += GLOBAL_LABELS['RETENIDO'][language];
                strDeta += "</td>";
                strDeta += "</tr>";
                strDeta += "</thead>";
                //ArrAcumulado
                // ArrRetencion.push(
                //     [
                //         "Prueba Bills 001 Account prueba CO 1",
                //         "FAC 00-3",
                //         10000.00,
                //         "22.256600",
                //         10000.00,
                //         "",
                //         "Enero"
                //     ]);
                // ArrRetencion.push(
                //     [
                //         "Prueba Bills 001 Account prueba CO 1",
                //         "FAC 00-3",
                //         50000.00,
                //         "22.256600",
                //         50000.00,
                //         "",
                //         "Marzo"
                //     ]);
                // ArrRetencion.push(
                //     [
                //         "Prueba Bills 002",
                //         "FAC 00-3",
                //         30000.00,
                //         "22.256600",
                //         20000.00,
                //         "",
                //         "Marzo"
                //     ]);
                strDeta += "<tbody>";
                log.debug('ArrRetencion', ArrRetencion);
                var jsonRetencionxmes = getJsonxMes(ArrRetencion);
                var arrMesesOrdenado = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'];
                for (var m = 0; m < arrMesesOrdenado.length; m++) {
                    if (jsonRetencionxmes[arrMesesOrdenado[m]] != undefined) {
                        var ArrAcumulado = AcumuladoPorConcept(jsonRetencionxmes[arrMesesOrdenado[m]]);
                        if (ArrAcumulado.length != null && ArrAcumulado.length != 0) {
                            noDataFound = false;
                            strDeta += DetalleRetencionxMes(ArrAcumulado);
                        }
                    }
                }
                if (noDataFound) {
                    RecordNoData();
                    return false;
                }

            } else {
                var strDeta = '';
                strDeta += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
                strDeta += "<thead>";
                strDeta += "<tr>";
                strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"25mm\">";
                strDeta += "<p>" + GLOBAL_LABELS['CONCEPTO'][language] + "</p>";
                strDeta += "</td>";
                strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
                strDeta += GLOBAL_LABELS['BASE'][language] + "<br/>";
                strDeta += GLOBAL_LABELS['RETENCION'][language];
                strDeta += "</td>";
                //  strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"30mm\">";
                //  strDeta += "<p>"+GLOBAL_LABELS['PORC'][language]+"</p>";
                //  strDeta += "</td>";
                strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
                strDeta += GLOBAL_LABELS['VALOR'][language] + "<br/>";
                strDeta += GLOBAL_LABELS['RETENIDO'][language];
                strDeta += "</td>";
                strDeta += "</tr>";
                strDeta += "</thead>";
                log.debug('ArrRetencion', ArrRetencion);

                strDeta += "<tbody>";
                var ArrAcumulado = [];
                ArrAcumulado = AcumuladoPorConcept(ArrRetencion);
                if (ArrAcumulado.length != null && ArrAcumulado.length != 0) {
                    strDeta += DetalleRetencion(ArrAcumulado);
                } else {
                    RecordNoData();
                    return false;
                }
            }

            strDeta += "</tbody>";
            // cierra la tabla
            strDeta += "</table>";

            strName += strDeta;

            //-------------------------------------------------------------------------------------------------------
            //Pie de p?gina del reporte
            //-------------------------------------------------------------------------------------------------------
            var strNpie = '';
            strNpie += "<p></p>";
            if (paramTyreten == 1) //RETEICA
            {
                strNpie += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
                strNpie += "<tr>";
                strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
                strNpie += GLOBAL_LABELS['RETEICA_PIE'][language] + municipality + ".";
                strNpie += "</td>";
                strNpie += "</tr>";
                strNpie += "</table>";
            } else {
                strNpie += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
                strNpie += "<tr>";
                strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
                strNpie += GLOBAL_LABELS['PIE'][language] + municipality + ".";
                strNpie += "</td>";
                strNpie += "</tr>";
                strNpie += "</table>";
            }
            strNpie += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            if (paramTyreten == 1) {
                strNpie += GLOBAL_LABELS['FirmaAutografaReteICA'][language];
            }
            if (paramTyreten == 2) {
                strNpie += GLOBAL_LABELS['FirmaAutografaReteFTE'][language];
            }
            if (paramTyreten == 3) {
                strNpie += GLOBAL_LABELS['FirmaAutografaReteIVA'][language];
            }
            strNpie += "</td>";
            strNpie += "</tr>";

            var auxDireccion = '';
            if (companyaddress != '') {
                var auxStr = companyaddress.split('\n');
                auxDireccion = auxStr[1];
                //companyaddress = 'xxxx';
            }
            var fecha_actual = new Date();
            var date = fecha_actual.getDate();
            if ((date + '').length == 1) {
                date = '0' + date;
            }
            DD = date;
            var mes = fecha_actual.getMonth() + 1;
            if ((mes + '').length == 1) {
                mes = '0' + mes;
            }
            MM = mes;
            YYYY = fecha_actual.getFullYear();
            fecha_actual = DD + "/" + MM + "/" + YYYY;

            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strNpie += GLOBAL_LABELS['FechaExpedicion'][language] + fecha_actual;
            strNpie += "</td>";
            strNpie += "</tr>";
            strNpie += "</table>";
            strNpie += "<p></p>";

            strName += strNpie;

            Final_string = "<?xml version=\"1.0\"?>\n<!DOCTYPE pdf PUBLIC \"-//big.faceless.org//report\" \"report-1.1.dtd\">\n";
            Final_string += '<pdf>';
            Final_string += '<head><style> body {size:A4}</style>';
            Final_string += '<macrolist>';
            Final_string += '<macro id=\"myfooter\">';
            Final_string += '<p align=\"right\">';
            Final_string += GLOBAL_LABELS['page'][language] + ' <pagenumber/> '+ GLOBAL_LABELS['of'][language] + ' <totalpages/>';
            Final_string += '</p>';
            Final_string += '</macro>';
            Final_string += '</macrolist>';
            Final_string += '</head>';
            Final_string += '<body footer=\"myfooter\" footer-height=\"20mm\">';
            Final_string += strName;
            Final_string += "</body>\n</pdf>";

            SaveFile();
        }

        function ObtieneRetencionReteICA() {

            //RETENCION EN LINEA
            var _cont = 0;
            var intDMinReg = Number(paramBucle) * 1000;
            var intDMaxReg = intDMinReg + 1000;
            var DbolStop = false;
            var infoTxt = '';
            var arrAuxiliar = new Array();
            var arrRetencion = new Array();

            var saved_search = search.load({
                id: 'customsearch_lmry_co_reteica_compras_pur'
            });

            if (featSubsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramsubsidi]
                });
                saved_search.filters.push(subsidiaryFilter);
            }

            //log.debug('formulPeriodFilters', formulPeriodFilters);

            var periodFilter = search.createFilter({
                name: "formulatext",
                formula: formulPeriodFilters,
                operator: search.Operator.IS,
                values: "1"
            });
            saved_search.filters.push(periodFilter);

            /* var entityFilter = search.createFilter({
                name: 'formulanumeric',
                formula: '{entity.id}',
                operator: search.Operator.EQUALTO,
                values: [paramVendor]
            });
            saved_search.filters.push(entityFilter); */
            var entityFilter = search.createFilter({
                name: 'formulanumeric',
                formula: '{vendor.internalid}',
                operator: search.Operator.EQUALTO,
                values: [paramVendor]
            });
            saved_search.filters.push(entityFilter); 

            if (featMulti) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMulti]
                });
                saved_search.filters.push(multibookFilter);
            }

            //Obtener ID del National Tax o Contributory Class
            var idNational_or_Contributory = search.createColumn({
                name: "formulatext",
                formula: "NVL(NVL({custrecord_lmry_br_transaction.custrecord_lmry_ntax.id}, {custrecord_lmry_br_transaction.custrecord_lmry_ccl.id}),'0')",
                label: "11. ID_National_o_Contributory"
            });
            saved_search.columns.push(idNational_or_Contributory);

            //Obtener label National Tax o Contributory Class
            var nameNational_or_Contributory = search.createColumn({
                name: "formulatext",
                formula: "(CASE WHEN ({custrecord_lmry_br_transaction.custrecord_lmry_ntax} is not null) THEN 'National Tax' WHEN ({custrecord_lmry_br_transaction.custrecord_lmry_ccl} is not null) THEN 'Contributory Class' ELSE '0'  END)",
                label: "12.National_o_Contributory"
            });
            saved_search.columns.push(nameNational_or_Contributory);


            //LATAM - BASE AMOUNT LOCAL CURRENCY
            var baseAmountCurrency = search.createColumn({
                name: "formulatext",
                formula: "{custrecord_lmry_br_transaction.custrecord_lmry_base_amount_local_currc}",
                label: "13. LATAM - BASE AMOUNT LOCAL CURRENCY"
            });
            saved_search.columns.push(baseAmountCurrency);


            //LATAM - AMOUNT LOCAL CURRENCY
            var amountLocalCurrency = search.createColumn({
                name: "formulatext",
                formula: "{custrecord_lmry_br_transaction.custrecord_lmry_amount_local_currency}",
                label: "14.LATAM - AMOUNT LOCAL CURRENCY"
            });
            saved_search.columns.push(amountLocalCurrency);

            //10.-Municipalidad
            var municipTransaction = search.createColumn({
                name: "internalid",
                join: "CUSTBODY_LMRY_MUNICIPALITY",
                label: "15. Municipality"
            });
            saved_search.columns.push(municipTransaction);

            var postingPeriodColumn = search.createColumn({
                name: 'postingperiod',
                label: "16. PERIODO"
            });
            saved_search.columns.push(postingPeriodColumn);

            var searchresult = saved_search.run();
            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    var intLength = objResult.length;
                    if (intLength != 1000) {
                        DbolStop = true;
                    }
                    //var arrLength = arrRetenciones.length;

                    for (var i = 0; i < intLength; i++) {

                        var columns = objResult[i].columns;
                        arrRetencion = new Array();
                        arrAuxiliar = new Array();
                        //log.debug('columns', columns.length);

                        // 0. C?DIGO WHT
                        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                            var nat_o_contib = objResult[i].getValue(columns[12]);
                            var idNat_o_contib = objResult[i].getValue(columns[11]);
                            // log.debug('nat_o_contib+idNat_o_contib',nat_o_contib+' - '+idNat_o_contib);
                            //Obtiene el WithHolding Description
                            arrAuxiliar[0] = obtenerWithHoldingDescription(nat_o_contib, idNat_o_contib);
                        } else
                            arrAuxiliar[0] = '';
                        // 1. TASA
                        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                            arrAuxiliar[1] = Number(objResult[i].getValue(columns[1])).toFixed(6);
                        } else
                            arrAuxiliar[1] = '0.00';
                        //log.error('arrAuxiliar[1]', arrAuxiliar[1]);
                        // 2. RAZ?N SOCIAL / INDIVIDUO
                        if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                        } else
                            arrAuxiliar[2] = '';


                        // 3. TIPO DE TRANSACCI?N
                        if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                        } else
                            arrAuxiliar[3] = '';

                        // 4. NUMERO DE TRANSACCI?N
                        if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                            arrAuxiliar[4] = objResult[i].getValue(columns[4]);
                        } else
                            arrAuxiliar[4] = '';

                        // 5. NOTA
                        if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                            arrAuxiliar[5] = objResult[i].getValue(columns[5]);
                        } else
                            arrAuxiliar[5] = '';

                        //G. exchange rate cabecera y multibook

                        if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != '') {
                            var ExchangerateAux = objResult[i].getValue(columns[10]);
                            ExchangerateC_S = exchange_rate(ExchangerateAux);
                        } else {
                            ExchangerateC_S = 1;
                        }

                        // 6. BASE IMPONIBLE
                        if (objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != '' && objResult[i].getValue(columns[13]) != 0) {
                            arrAuxiliar[6] = objResult[i].getValue(columns[13]);
                        } else {
                            if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                arrAuxiliar[6] = (parseFloat(objResult[i].getValue(columns[6]))) * (Number(ExchangerateC_S));
                            } else {
                                arrAuxiliar[6] = 0;
                            }
                        }

                        // 7. RETENCION
                        if (objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != '' && objResult[i].getValue(columns[14]) != 0) {
                            arrAuxiliar[7] = objResult[i].getValue(columns[14]);
                        } else {
                            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {

                                arrAuxiliar[7] = (parseFloat(objResult[i].getValue(columns[7]))) * (Number(ExchangerateC_S));

                            } else {
                                arrAuxiliar[7] = 0;
                            }
                        }

                        if (arrAuxiliar[3] == "VendBill" || arrAuxiliar[3] == "Factura" ) {
                            arrAuxiliar[7] = Math.abs(arrAuxiliar[7]);
                        } else if (arrAuxiliar[3] == "VendCred" || arrAuxiliar[3] == "Crédito de factura") {
                            arrAuxiliar[7] = -Math.abs(arrAuxiliar[7]);
                        }

                        //8. FECHA
                        if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -') {
                            arrAuxiliar[8] = objResult[i].getValue(columns[8]);
                        } else
                            arrAuxiliar[8] = '';

                        //9. DESCRIPCION
                        if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -') {
                            arrAuxiliar[9] = Number(objResult[i].getValue(columns[9])).toFixed(6);
                        } else
                            arrAuxiliar[9] = '';

                        //10. MUNICIPALIDAD
                        if (objResult[i].getValue(columns[15]) != null && objResult[i].getValue(columns[15]) != '- None -') {
                            arrAuxiliar[10] = objResult[i].getValue(columns[15]);
                        } else
                            arrAuxiliar[10] = '';

                        //11. MUNICIPALIDAD
                        if (objResult[i].getValue(columns[16]) != null && objResult[i].getValue(columns[16]) != '- None -') {
                            arrAuxiliar[11] = objResult[i].getValue(columns[16]);
                        } else
                            arrAuxiliar[11] = '';

                        // infoTxt = infoTxt +
                        //columna0 + columna1 + columna2 + columna3 + columna4 + columna5 + columna6 + columna7 + columna8 +
                        //columna9 +'\r\n';
                        //  log.debug("Codigo - Label",objResult[i].getValue(columns[11])+ ' - '+objResult[i].getValue(columns[12]));                 


                        //COLUMNA DE RETENCIONES IMPORTANTES(Concepto, factura, base retención, porc, valor retenido)
                        arrRetencion[0] = arrAuxiliar[0];
                        arrRetencion[1] = arrAuxiliar[4];
                        arrRetencion[2] = arrAuxiliar[6];
                        arrRetencion[3] = arrAuxiliar[9];
                        arrRetencion[4] = arrAuxiliar[7];
                        arrRetencion[5] = arrAuxiliar[10];
                        arrRetencion[6] = jsonNameMonth[arrAuxiliar[11]];

                        //log.debug("arrRetencion[0]",arrRetencion[0]);
                        //  log.debug('Elementos',arrRetencion[0]+' - '+arrRetencion[1]+' - '+arrRetencion[2]+' - '+arrRetencion[3]+' - '+arrRetencion[4]);
                        ArrReteAux[_cont] = arrRetencion;
                        _cont++;
                    }
                    //clcular tamaÃ±o de string temporal
                    var string_size_in_bytes = lengthInUtf8Bytes(infoTxt);

                    if (!DbolStop) {
                        if (objContext.getRemainingUsage() <= 500 || string_size_in_bytes >= file_size) {
                            paramCont = Number(paramCont) + 1;
                            paramBucle = Number(paramBucle) + 1;
                            ArrReteAux;
                            //SaveFile();
                            LlamarSchedule(paramCont, paramBucle);
                            flag = true;
                            return true;
                        } else {
                            intDMinReg = intDMaxReg;
                            intDMaxReg += 50;
                            paramBucle = Number(paramBucle) + 1;
                        }
                    } else {
                        if (paramCont != 0) {
                            paramCont = Number(paramCont) + 1;
                        }
                    }
                } else {
                    DbolStop = true;
                }
            }
            log.debug('arrRetencion 1 ', ArrReteAux);
            log.debug('arrRetencion 1 LEN', ArrReteAux.length);

            var arrRetencion = new Array();

            //  AGREGANDO LAS RETENCIONES RETEICA (CABECERA)
            arrRetencion = ObtieneRetencionCabecera('customsearch_lmry_co_reteica_compras', _cont);

            // log.debug('arrRetencion 2', arrRetencion);
            // log.debug('arrRetencion 2 len', arrRetencion.length);
            return arrRetencion;
        }


        function ObtieneRetencionReteFTE() {
            //RETENCION EN LINEA
            var _cont = 0;
            var intDMinReg = Number(paramBucle) * 1000;
            var intDMaxReg = intDMinReg + 1000;
            var DbolStop = false;
            var infoTxt = '';
            var arrAuxiliar = new Array();
            var arrRetencion = new Array();

            log.debug('param 1', {
                paramsubsidi: paramsubsidi,
                paramVendor: paramVendor,
                formulPeriodFilters: formulPeriodFilters,
                paramMulti: paramMulti
            });

            var saved_search = search.load({
                id: 'customsearch_lmry_co_retefte_compras_pur'
            });

            if (featSubsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramsubsidi]
                });
                saved_search.filters.push(subsidiaryFilter);
            }

            /* var entityFilter = search.createFilter({
                name: 'formulanumeric',
                formula: '{entity.id}',
                operator: search.Operator.EQUALTO,
                values: [paramVendor]
            });
            saved_search.filters.push(entityFilter); */
            var entityFilter = search.createFilter({
                name: 'formulanumeric',
                formula: '{vendor.internalid}',
                operator: search.Operator.EQUALTO,
                values: [paramVendor]
            });
            saved_search.filters.push(entityFilter);

            var periodFilter = search.createFilter({
                name: "formulatext",
                formula: formulPeriodFilters,
                operator: search.Operator.IS,
                values: "1"
            });
            saved_search.filters.push(periodFilter);

            if (featMulti) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMulti]
                });
                saved_search.filters.push(multibookFilter);
            }

            //Obtener ID del National Tax o Contributory Class
            var idNational_or_Contributory = search.createColumn({
                name: "formulatext",
                formula: "NVL(NVL({custrecord_lmry_br_transaction.custrecord_lmry_ntax.id}, {custrecord_lmry_br_transaction.custrecord_lmry_ccl.id}),'0')",
                label: "ID_National_o_Contributory"
            });
            saved_search.columns.push(idNational_or_Contributory);

            //Obtener label National Tax o Contributory Class
            var nameNational_or_Contributory = search.createColumn({
                name: "formulatext",
                formula: "(CASE  WHEN ({custrecord_lmry_br_transaction.custrecord_lmry_ntax} is not null) THEN 'National Tax' WHEN ({custrecord_lmry_br_transaction.custrecord_lmry_ccl} is not null) THEN 'Contributory Class' ELSE '0'  END)",
                label: "National_o_Contributory"
            });
            saved_search.columns.push(nameNational_or_Contributory);

            //LATAM - BASE AMOUNT LOCAL CURRENCY
            var baseAmountCurrency = search.createColumn({
                name: "formulatext",
                formula: "{custrecord_lmry_br_transaction.custrecord_lmry_base_amount_local_currc}",
                label: "LATAM - BASE AMOUNT LOCAL CURRENCY"
            });
            saved_search.columns.push(baseAmountCurrency);

            //LATAM - AMOUNT LOCAL CURRENCY
            var amountLocalCurrency = search.createColumn({
                name: "formulatext",
                formula: "{custrecord_lmry_br_transaction.custrecord_lmry_amount_local_currency}",
                label: "LATAM - AMOUNT LOCAL CURRENCY"
            });
            saved_search.columns.push(amountLocalCurrency);

            var postingPeriodColumn = search.createColumn({
                name: 'postingperiod',
                label: "11. PERIODO"
            });
            saved_search.columns.push(postingPeriodColumn);


            var searchresult = saved_search.run();

            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    var intLength = objResult.length;
                    if (intLength != 1000) {
                        DbolStop = true;
                    }
                    //var arrLength = arrRetenciones.length;

                    for (var i = 0; i < intLength; i++) {
                        var columns = objResult[i].columns;
                        arrRetencion = new Array();
                        arrAuxiliar = new Array();
                        // 0. C?DIGO WHT
                        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                            var label = objResult[i].getValue(columns[12]);
                            var idLabel = objResult[i].getValue(columns[11]);
                            //Obtiene el WithHolding Description
                            arrAuxiliar[0] = obtenerWithHoldingDescription(label, idLabel);
                        } else
                            arrAuxiliar[0] = '';

                        // 1. TASA
                        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                            arrAuxiliar[1] = Number(objResult[i].getValue(columns[1])).toFixed(6);
                        } else
                            arrAuxiliar[1] = '0.00';

                        // 2. RAZ?N SOCIAL / INDIVIDUO
                        if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                        } else
                            arrAuxiliar[2] = '';

                        // 3. TIPO DE TRANSACCI?N
                        if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                        } else
                            arrAuxiliar[3] = '';

                        // 4. NUMERO DE TRANSACCI?N
                        if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                            arrAuxiliar[4] = objResult[i].getValue(columns[4]);
                        } else
                            arrAuxiliar[4] = '';

                        // 5. NOTA
                        if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                            arrAuxiliar[5] = objResult[i].getValue(columns[5]);
                        } else
                            arrAuxiliar[5] = '';


                        //G. exchange rate cabecera y multibook

                        if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != '') {
                            var ExchangerateAux = objResult[i].getValue(columns[10]);
                            ExchangerateC_S = exchange_rate(ExchangerateAux);
                        } else {
                            ExchangerateC_S = 1;
                        }

                        // 6. BASE IMPONIBLE
                        if (objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != '' && objResult[i].getValue(columns[13]) != 0) {
                            arrAuxiliar[6] = objResult[i].getValue(columns[13]);
                        } else {
                            if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {

                                arrAuxiliar[6] = (parseFloat(objResult[i].getValue(columns[6]))) * (Number(ExchangerateC_S));

                            } else {
                                arrAuxiliar[6] = 0;
                            }
                        }


                        // 7. RETENCION
                        if (objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != '' && objResult[i].getValue(columns[14]) != 0) {
                            arrAuxiliar[7] = objResult[i].getValue(columns[14]);
                        } else {
                            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {

                                arrAuxiliar[7] = (parseFloat(objResult[i].getValue(columns[7]))) * (Number(ExchangerateC_S));

                            } else {
                                arrAuxiliar[7] = 0;
                            }
                        }


                        if (arrAuxiliar[3] == "VendBill" || arrAuxiliar[3] == "Factura" ) {
                            arrAuxiliar[7] = Math.abs(arrAuxiliar[7]);
                        } else if (arrAuxiliar[3] == "VendCred" || arrAuxiliar[3] == "Crédito de factura") {
                            arrAuxiliar[7] = -Math.abs(arrAuxiliar[7]);
                        }

                        //8. FECHA
                        if (objResult[i].getValue(columns[15]) != null && objResult[i].getValue(columns[15]) != '- None -') {
                            arrAuxiliar[8] = objResult[i].getValue(columns[15]);
                        } else
                            arrAuxiliar[8] = '';

                        //9. DESCRIPCION
                        if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -') {
                            arrAuxiliar[9] = Number(objResult[i].getValue(columns[9])).toFixed(6);
                        } else
                            arrAuxiliar[9] = '';

                        // infoTxt = infoTxt +
                        //columna0 + columna1 + columna2 + columna3 + columna4 + columna5 + columna6 + columna7 + columna8 +
                        //columna9 +'\r\n';

                        //  log.debug("Codigo - Label",objResult[i].getValue(columns[11])+ ' - '+objResult[i].getValue(columns[12]));                 



                        //COLUMNA DE RETENCIONES IMPORTANTES(Concepto, factura, base retención, porc, valor retenido)
                        arrRetencion[0] = arrAuxiliar[0];
                        arrRetencion[1] = arrAuxiliar[4];
                        arrRetencion[2] = arrAuxiliar[6];
                        arrRetencion[3] = arrAuxiliar[9];
                        arrRetencion[4] = arrAuxiliar[7];
                        arrRetencion[5] = '';
                        arrRetencion[6] = jsonNameMonth[arrAuxiliar[8]];

                        //  log.debug('Elementos',arrRetencion[0]+' - '+arrRetencion[1]+' - '+arrRetencion[2]+' - '+arrRetencion[3]+' - '+arrRetencion[4]);

                        ArrReteAux[_cont] = arrRetencion;
                        _cont++;

                    }
                    //clcular tamaÃ±o de string temporal
                    var string_size_in_bytes = lengthInUtf8Bytes(infoTxt);

                    if (!DbolStop) {
                        if (objContext.getRemainingUsage() <= 500 || string_size_in_bytes >= file_size) {
                            paramCont = Number(paramCont) + 1;
                            paramBucle = Number(paramBucle) + 1;
                            ArrReteAux;
                            //SaveFile();
                            LlamarSchedule(paramCont, paramBucle);
                            flag = true;
                            return true;
                        } else {
                            intDMinReg = intDMaxReg;
                            intDMaxReg += 50;
                            paramBucle = Number(paramBucle) + 1;
                        }
                    } else {
                        if (paramCont != 0) {
                            paramCont = Number(paramCont) + 1;
                        }
                    }
                } else {
                    DbolStop = true;
                }
            }
            var arrRetencion = new Array();

            log.debug('ArrReteAux', ArrReteAux);
            log.debug('ArrReteAux', ArrReteAux.length);
            //  AGREGANDO LAS RETENCIONES RETEICA (CABECERA)
            arrRetencion = ObtieneRetencionCabecera('customsearch_lmry_co_retefte_compras', _cont);
            //log.debug('arrRetencion', arrRetencion);

            return arrRetencion;
        }

        function ObtieneRetencionReteIVA() {

            var _cont = 0;
            var intDMinReg = Number(paramBucle) * 1000;
            var intDMaxReg = intDMinReg + 1000;
            var DbolStop = false;
            var infoTxt = '';
            var arrAuxiliar = new Array();
            var arrRetencion = new Array();

            var saved_search = search.load({
                id: 'customsearch_lmry_co_reteiva_compras_pur'
            });

            if (featSubsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramsubsidi]
                });
                saved_search.filters.push(subsidiaryFilter);
            }


            /* var entityFilter = search.createFilter({
                name: 'formulanumeric',
                formula: '{entity.id}',
                operator: search.Operator.EQUALTO,
                values: [paramVendor]
            });
            saved_search.filters.push(entityFilter); */
            var entityFilter = search.createFilter({
                name: 'formulanumeric',
                formula: '{vendor.internalid}',
                operator: search.Operator.EQUALTO,
                values: [paramVendor]
            });
            saved_search.filters.push(entityFilter);

            var periodFilter = search.createFilter({
                name: "formulatext",
                formula: formulPeriodFilters,
                operator: search.Operator.IS,
                values: "1"
            });
            saved_search.filters.push(periodFilter);


            if (featMulti) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMulti]
                });
                saved_search.filters.push(multibookFilter);
            }

            //Obtener ID del National Tax o Contributory Class
            var idNational_or_Contributory = search.createColumn({
                name: "formulatext",
                formula: "NVL(NVL({custrecord_lmry_br_transaction.custrecord_lmry_ntax.id}, {custrecord_lmry_br_transaction.custrecord_lmry_ccl.id}),'0')",
                label: "ID_National_o_Contributory"
            });
            saved_search.columns.push(idNational_or_Contributory);

            //Obtener label National Tax o Contributory Class
            var nameNational_or_Contributory = search.createColumn({
                name: "formulatext",
                formula: "(CASE  WHEN ({custrecord_lmry_br_transaction.custrecord_lmry_ntax} is not null) THEN 'National Tax' WHEN ({custrecord_lmry_br_transaction.custrecord_lmry_ccl} is not null) THEN 'Contributory Class' ELSE '0'  END)",
                label: "National_o_Contributory"
            });
            saved_search.columns.push(nameNational_or_Contributory);

            //LATAM - BASE AMOUNT LOCAL CURRENCY
            var baseAmountCurrency = search.createColumn({
                name: "formulatext",
                formula: "{custrecord_lmry_br_transaction.custrecord_lmry_base_amount_local_currc}",
                label: "LATAM - BASE AMOUNT LOCAL CURRENCY"
            });
            saved_search.columns.push(baseAmountCurrency);


            //LATAM - AMOUNT LOCAL CURRENCY
            var amountLocalCurrency = search.createColumn({
                name: "formulatext",
                formula: "{custrecord_lmry_br_transaction.custrecord_lmry_amount_local_currency}",
                label: "LATAM - AMOUNT LOCAL CURRENCY"
            });
            saved_search.columns.push(amountLocalCurrency);

            var postingPeriodColumn = search.createColumn({
                name: 'postingperiod',
                label: "11. PERIODO"
            });
            saved_search.columns.push(postingPeriodColumn);

            var searchresult = saved_search.run();

            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    var intLength = objResult.length;
                    if (intLength != 1000) {
                        DbolStop = true;
                    }
                    //var arrLength = arrRetenciones.length;

                    for (var i = 0; i < intLength; i++) {
                        var columns = objResult[i].columns;
                        arrRetencion = new Array();
                        arrAuxiliar = new Array();
                        // 0. C?DIGO WHT
                        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                            var label = objResult[i].getValue(columns[12]);
                            var idLabel = objResult[i].getValue(columns[11]);
                            //Obtiene el WithHolding Description
                            arrAuxiliar[0] = obtenerWithHoldingDescription(label, idLabel);
                        } else
                            arrAuxiliar[0] = '';

                        // 1. TASA
                        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                            arrAuxiliar[1] = Number(objResult[i].getValue(columns[1])).toFixed(6);
                        } else
                            arrAuxiliar[1] = '0.00';

                        // 2. RAZ?N SOCIAL / INDIVIDUO
                        if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                        } else
                            arrAuxiliar[2] = '';


                        // 3. TIPO DE TRANSACCI?N
                        if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                        } else
                            arrAuxiliar[3] = '';

                        // 4. NUMERO DE TRANSACCI?N
                        if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                            arrAuxiliar[4] = objResult[i].getValue(columns[4]);
                        } else
                            arrAuxiliar[4] = '';

                        // 5. NOTA
                        if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                            arrAuxiliar[5] = objResult[i].getValue(columns[5]);
                        } else
                            arrAuxiliar[5] = '';

                        //G. exchange rate cabecera y multibook
                        if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != '') {
                            var ExchangerateAux = objResult[i].getValue(columns[10]);
                            ExchangerateC_S = exchange_rate(ExchangerateAux);
                        } else {
                            ExchangerateC_S = 1;
                        }


                        // 6. BASE IMPONIBLE
                        if (objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != '' && objResult[i].getValue(columns[13]) != 0) {
                            arrAuxiliar[6] = objResult[i].getValue(columns[13]);
                        } else {
                            if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                arrAuxiliar[6] = (parseFloat(objResult[i].getValue(columns[6]))) * (Number(ExchangerateC_S));
                            } else {
                                arrAuxiliar[6] = 0;
                            }
                        }

                        // 7. RETENCION
                        if (objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != '' && objResult[i].getValue(columns[14]) != 0) {
                            arrAuxiliar[7] = objResult[i].getValue(columns[14]);
                        } else {
                            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {
                                arrAuxiliar[7] = (parseFloat(objResult[i].getValue(columns[7]))) * (Number(ExchangerateC_S));
                            } else {
                                arrAuxiliar[7] = 0;
                            }
                        } 

                        if (arrAuxiliar[3] == "VendBill" || arrAuxiliar[3] == "Factura" ) {
                            arrAuxiliar[7] = Math.abs(arrAuxiliar[7]);
                        } else if (arrAuxiliar[3] == "VendCred" || arrAuxiliar[3] == "Crédito de factura") {
                            arrAuxiliar[7] = -Math.abs(arrAuxiliar[7]);
                        }

                        //8. FECHA
                        if (objResult[i].getValue(columns[15]) != null && objResult[i].getValue(columns[15]) != '- None -') {
                            arrAuxiliar[8] = objResult[i].getValue(columns[15]);
                        } else
                            arrAuxiliar[8] = '';

                        //9. DESCRIPCION
                        if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -') {
                            arrAuxiliar[9] = Number(objResult[i].getValue(columns[9])).toFixed(6);
                        } else
                            arrAuxiliar[9] = '';

                        // infoTxt = infoTxt +
                        //columna0 + columna1 + columna2 + columna3 + columna4 + columna5 + columna6 + columna7 + columna8 +
                        //columna9 +'\r\n';

                        //  log.debug("Codigo - Label",objResult[i].getValue(columns[11])+ ' - '+objResult[i].getValue(columns[12]));                 


                        //COLUMNA DE RETENCIONES IMPORTANTES(Concepto, factura, base retención, porc, valor retenido)
                        arrRetencion[0] = arrAuxiliar[0];
                        arrRetencion[1] = arrAuxiliar[4];
                        arrRetencion[2] = arrAuxiliar[6];
                        arrRetencion[3] = arrAuxiliar[9];
                        arrRetencion[4] = arrAuxiliar[7];
                        arrRetencion[5] = '';
                        arrRetencion[6] = jsonNameMonth[arrAuxiliar[8]];

                        //  log.debug('Elementos',arrRetencion[0]+' - '+arrRetencion[1]+' - '+arrRetencion[2]+' - '+arrRetencion[3]+' - '+arrRetencion[4]);

                        ArrReteAux[_cont] = arrRetencion;
                        _cont++;

                    }
                    //clcular tamaÃ±o de string temporal
                    var string_size_in_bytes = lengthInUtf8Bytes(infoTxt);

                    if (!DbolStop) {
                        if (objContext.getRemainingUsage() <= 500 || string_size_in_bytes >= file_size) {
                            paramCont = Number(paramCont) + 1;
                            paramBucle = Number(paramBucle) + 1;
                            ArrReteAux;
                            //SaveFile();
                            LlamarSchedule(paramCont, paramBucle);
                            flag = true;
                            return true;
                        } else {
                            intDMinReg = intDMaxReg;
                            intDMaxReg += 50;
                            paramBucle = Number(paramBucle) + 1;
                        }
                    } else {
                        if (paramCont != 0) {
                            paramCont = Number(paramCont) + 1;
                        }
                    }
                } else {
                    DbolStop = true;
                }
            }
            var arrRetencion = new Array();
            //  AGREGANDO LAS RETENCIONES RETEICA (CABECERA)
            arrRetencion = ObtieneRetencionCabecera('customsearch_lmry_co_reteiva_compras', _cont);
            //log.debug('arrRetencion', arrRetencion);
            return arrRetencion;
        }

        function ObtieneRetencionCabecera(pBusqueda, _cont) {
            //	try
            //	{
            // Control de Memoria
            var intDMaxReg = 1000;
            var intDMinReg = 0;
            var arrAuxiliar = new Array();
            var arrRetencion = new Array();
            var aux = new Array();
            // Exedio las unidades
            var Dusager = false;
            var DbolStop = false;
            //log.debug('_cont', _cont);

            var usageRemaining = objContext.getRemainingUsage();

            var param = {
                pBusqueda: pBusqueda,
                _cont: _cont,
                paramsubsidi: paramsubsidi,
                paramVendor: paramVendor,
                featSubsi: featSubsi,
                featMulti: featMulti
            }
            log.debug('param rtefte', param);

            var savedsearch = search.load({
                id: pBusqueda
            });

            // Valida si es OneWorld
            if (featSubsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramsubsidi]
                });
                savedsearch.filters.push(subsidiaryFilter);
            }

            // log.debug('formulPeriodFilters',formulPeriodFilters);     

            var periodFilter = search.createFilter({
                name: "formulatext",
                formula: formulPeriodFilters,
                operator: search.Operator.IS,
                values: "1"
            });
            savedsearch.filters.push(periodFilter);

            /* var entityFilter = search.createFilter({
                name: 'formulanumeric',
                formula: '{entity.id}',
                operator: search.Operator.EQUALTO,
                values: [paramVendor]
            });
            savedsearch.filters.push(entityFilter); */
            var entityFilter = search.createFilter({
                name: 'formulanumeric',
                formula: '{vendor.internalid}',
                operator: search.Operator.EQUALTO,
                values: [paramVendor]
            });
            savedsearch.filters.push(entityFilter); 
                

            log.debug('paso 1','');

            //NO ESTOY SEGURO SI ESTE CODIGO ESTA BIEN
            //savedsearch.addColumn(new nlobjSearchColumn("formulatext",null,"GROUP").setFormula("{type.id}"));
            var typeIdColumn = search.createColumn({
                name: 'formulatext',
                summary: 'GROUP',
                formula: '{type.id}'
            });
            savedsearch.columns.push(typeIdColumn);

            log.debug('paso 2','');

            //9.-Municipalidad
            var municipTransaction = search.createColumn({
                name: "internalid",
                summary: 'GROUP',
                join: "CUSTBODY_LMRY_MUNICIPALITY",
                label: "9. Municipality"
            });
            savedsearch.columns.push(municipTransaction);

            log.debug('paso 3','');

            if (featMulti == true || featMulti == 'T') {
                //savedsearch.addFilter(new nlobjSearchFilter('accountingbook', 'accountingtransaction', 'anyof', paramMulti));
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.ANYOF,
                    values: [paramMulti]
                });
                savedsearch.filters.push(multibookFilter);

                //savedsearch.addColumn(new nlobjSearchColumn("formulacurrency",null,"SUM").setFormula("{accountingtransaction.amount}"));
                var amountCurrencyColumn = search.createColumn({
                    name: 'formulacurrency',
                    summary: 'SUM',
                    formula: '{accountingtransaction.amount}'
                });
                savedsearch.columns.push(amountCurrencyColumn);
            }

            var postingPeriodColumn = search.createColumn({
                name: 'postingperiod',
                summary: 'GROUP',
                label: "11. PERIODO"
            });
            savedsearch.columns.push(postingPeriodColumn);

            var searchresult = savedsearch.run();


            log.debug('llego hasta aqui','');
            while (!DbolStop && objContext.getRemainingUsage() > 200) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
                //nlapiLogExecution('ERROR', 'objResult-> ', objResult);
                var intLength = objResult.length;
                if (objResult != null && intLength > 0) {

                    var intLength = objResult.length;
                    if (intLength != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < intLength; i++) {
                        var columns = objResult[i].columns;

                        if (i == intLength) {
                            break;
                        }
                        
                        arrRetencion = new Array();
                        arrAuxiliar = new Array();

                        var tasa = calcular_tasa(objResult[i].getValue(columns[1]));
                        var desc = calcular_desc(objResult[i].getValue(columns[1]));

                        //0. C?DIGO WHT
                        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'undefined') {
                            arrAuxiliar[0] = objResult[i].getValue(columns[1]);
                        } else {
                            arrAuxiliar[0] = '';
                        }
                        //1. TASA
                        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'undefined') {
                            arrAuxiliar[1] = tasa;
                        } else {
                            arrAuxiliar[1] = '0.00';
                        }
                        //4. NuMERO DE TRANSACCI?N
                        if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'undefined') {
                            arrAuxiliar[2] = objResult[i].getValue(columns[4]);
                        } else {
                            arrAuxiliar[2] = '';
                        }
                        //5. Base Imponible
                        if (featMulti == true || featMulti == 'T') {
                            if (Number(objResult[i].getValue(columns[10])) < 0) {
                                arrAuxiliar[3] = (parseFloat(Number(objResult[i].getValue(columns[10]) * (-1))));
                            } else {
                                arrAuxiliar[3] = (parseFloat(objResult[i].getValue(columns[10])));
                            }
                        } else {
                            if (Number(objResult[i].getValue(columns[5])) < 0) {
                                arrAuxiliar[3] = (parseFloat(Number(objResult[i].getValue(columns[5]) * (-1))));
                            } else {
                                arrAuxiliar[3] = (parseFloat(objResult[i].getValue(columns[5])));
                            }
                        }
                        //6.RETENCIoN
                        if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'undefined') {
                            arrAuxiliar[4] = (parseFloat(objResult[i].getValue(columns[6])));
                            if (objResult[i].getValue(columns[8]) == 'VendCred') {
                                arrAuxiliar[4] = (parseFloat(arrAuxiliar[4] * -1));
                            }
                        } else {
                            arrAuxiliar[4] = 0;
                        }
                        //7.DESCRIPTION
                        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'undefined') {
                            arrAuxiliar[5] = desc;
                        } else {
                            arrAuxiliar[5] = '';
                        }
                        // Municipalidad id
                        if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'undefined') {
                            arrAuxiliar[6] = objResult[i].getValue(columns[9]);
                        } else {
                            arrAuxiliar[6] = '';
                        }
                        // POSTING PERIOD
                        if (featMulti == true || featMulti == 'T') {
                            if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'undefined') {
                                arrAuxiliar[7] = objResult[i].getValue(columns[11]);
                            } else {
                                arrAuxiliar[7] = '';
                            }
                        } else {
                            if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 'undefined') {
                                arrAuxiliar[7] = objResult[i].getValue(columns[10]);
                            } else {
                                arrAuxiliar[7] = '';
                            }
                        }
                        
                        //COLUMNA DE RETENCIONES IMPORTANTES(Concepto, factura, base retención, porc, valor retenido)
                        arrRetencion[0] = arrAuxiliar[0];
                        arrRetencion[1] = arrAuxiliar[2];
                        arrRetencion[2] = arrAuxiliar[3];
                        arrRetencion[3] = arrAuxiliar[5];
                        arrRetencion[4] = arrAuxiliar[4];
                        arrRetencion[5] = arrAuxiliar[6];
                        arrRetencion[6] = jsonNameMonth[arrAuxiliar[7]];

                        //   log.debug('Concepto - Nro Factura - Base Retencion - Porc - Valor Retenido',arrRetencion[0]+' - '+arrRetencion[1]+' - '+arrRetencion[2]+' - '+arrRetencion[3]+' - '+arrRetencion[4]);

                        //nlapiLogExecution('DEBUG', 'arrAuxiliar-> ',arrAuxiliar);
                        ArrReteAux[_cont] = arrRetencion;
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
            //log.debug('ArrReteAux.length', ArrReteAux.length);    
            return ArrReteAux;
        }

        function getJsonxMes(ArrayRetencion) {
            var jsonReturn = {};
            for (var i = 0; i < ArrayRetencion.length; i++) {
                if (jsonReturn[ArrayRetencion[i][6]] != undefined) {
                    jsonReturn[ArrayRetencion[i][6]].push(ArrayRetencion[i]);
                } else {
                    jsonReturn[ArrayRetencion[i][6]] = [ArrayRetencion[i]];
                }
            }
            return jsonReturn;
        }

        function AcumuladoPorConcept(ArrRetAux) {
            var arrMoment = new Array();
            var cont = -1;
            for (var i = 0; i <= ArrRetAux.length - 1; i++) {
                var encontro = false;
                //Recorrer el arreglo de acumulado x Concepto
                for (var j = 0; j <= arrMoment.length - 1; j++) {
                    //Es el mismo concepto
                    if (arrMoment[j][0] == ArrRetAux[i][0] && arrMoment[j][2] == ArrRetAux[i][3]) {
                        arrMoment[j][1] += parseFloat((Number(ArrRetAux[i][2])).toFixed(2));
                        arrMoment[j][2] = ArrRetAux[i][3];
                        arrMoment[j][3] += parseFloat((Number(ArrRetAux[i][4])).toFixed(2));
                        arrMoment[j][4] = ArrRetAux[i][6];
                        // log.error('Base - Porcentaje - Retencion', arrMoment[j][1]+' - '+arrMoment[j][2]+' - '+arrMoment[j][3]);
                        encontro = true;
                    }
                }
                //Si no encontro el valor
                if (!encontro) {
                    cont++;
                    arrMoment[cont] = new Array();
                    arrMoment[cont][0] = ArrRetAux[i][0];
                    arrMoment[cont][1] = parseFloat((Number(ArrRetAux[i][2])).toFixed(2));
                    arrMoment[cont][2] = ArrRetAux[i][3];
                    arrMoment[cont][3] = parseFloat((Number(ArrRetAux[i][4])).toFixed(2));
                    arrMoment[cont][4] = ArrRetAux[i][6];

                    // log.debug('arrMoment[cont]',arrMoment[cont][0]+' - '+arrMoment[cont][1]+' - '+arrMoment[cont][2]+' - '+arrMoment[cont][3]);
                }

            }
            //Probar los datos
            //log.debug('Cantidad de elementos',arrMoment.length);
            // for (var k = 0; k <= arrMoment.length - 1; k++) {
            //     log.debug('Base - Porcentaje - Retencion', arrMoment[k][1] + ' - ' + arrMoment[k][2] + ' - ' + arrMoment[k][3]);
            // }
            return arrMoment;
        }




        function exchange_rate(exchangerate) {
            var auxiliar = ('' + exchangerate).split('&');
            var final = '';

            if (featMulti) {
                var id_libro = auxiliar[0].split('|');
                var exchange_rate = auxiliar[1].split('|');

                for (var i = 0; i < id_libro.length; i++) {
                    if (Number(id_libro[i]) == Number(paramMulti)) {
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

        function lengthInUtf8Bytes(str) {
            var m = encodeURIComponent(str).match(/%[89ABab]/g);
            return str.length + (m ? m.length : 0);
        }

        function ObtenerDatosSubsidiaria() {
            log.error('ENTRO', 'entro ObtenerDatosSubsidiaria');

            var configpage = config.load({
                type: config.Type.COMPANY_INFORMATION
            });
            if (featSubsi) {
                companyname = ObtainNameSubsidiaria(paramsubsidi);
                companyruc = ObtainFederalIdSubsidiaria(paramsubsidi);
                companyaddress = ObtainAddressIdSubsidiaria(paramsubsidi);
                companycity = ObtainCityIdSubsidiaria(paramsubsidi);
            } else {
                companyruc = configpage.getFieldValue('employerid');
                companyname = configpage.getFieldValue('legalname');
                companyaddress = configpage.getFieldValue('address1');
            }
            // companyruc = companyruc.replace(' ', '');
            // companyname = companyname.replace(' ', '');
            // companyaddress = companyaddress.replace(' ', '');
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
                libreria.sendMail(LMRY_script, ' [ ObtainAddressIdSubsidiaria ] ' + err);
            }
            return '';
        }

        function ObtainCityIdSubsidiaria(subsidiary) {
            try {
                if (subsidiary != '' && subsidiary != null) {
                    var SubsidiCity = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: subsidiary,
                        columns: ['city']
                    });
                    return SubsidiCity.city

                }
            } catch (err) {
                libreria.sendMail(LMRY_script, ' [ ObtainAddressIdSubsidiaria ] ' + err);
            }
            return '';
        }

        function ValidateCountry(subsidiary) {
            try {
                if (subsidiary != '' && subsidiary != null) {
                    var country_obj = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: subsidiary,
                        columns: ['country']
                    });
                    if (country_obj.country[0].value == 'MX') {
                        return true;
                    }
                }
            } catch (err) {
                libreria.sendMail(LMRY_script, ' [ ValidateCountry ] ' + err);
            }
            return false;
        }

        //-------------------------------------------------------------------------------------------------------
        //Obtiene a?o y mes del periodo
        //-------------------------------------------------------------------------------------------------------
        function Periodo(periodo) {
            periodo = periodo + '';

            var auxmess = '';
            switch (periodo) {
                case '01':
                    auxmess = 'Jan';
                    break;
                case '02':
                    auxmess = 'Feb';
                    break;
                case '03':
                    auxmess = 'Mar';
                    break;
                case '04':
                    auxmess = 'Apr';
                    break;
                case '05':
                    auxmess = 'May';
                    break;
                case '06':
                    auxmess = 'Jun';
                    break;
                case '07':
                    auxmess = 'Jul';
                    break;
                case '08':
                    auxmess = 'Aug';
                    break;
                case '09':
                    auxmess = 'Sep';
                    break;
                case '10':
                    auxmess = 'Oct';
                    break;
                case '11':
                    auxmess = 'Nov';
                    break;
                case '12':
                    auxmess = 'Dec';
                    break;
            }

            return auxmess;
        }

        //-------------------------------------------------------------------------------------------------------
        //Concadena al aux un caracter segun la cantidad indicada
        //-------------------------------------------------------------------------------------------------------
        function RellenaTexto(aux, TotalDigitos, TipoCaracter) {
            var Numero = aux.toString();
            var mon_len = parseInt(TotalDigitos) - Numero.length;

            if (mon_len < 0) {
                mon_len = mon_len * -1;
            }
            // Solo para el tipo caracter
            if (TipoCaracter == 'C') {
                mon_len = parseInt(mon_len) + 1;
            }

            if (Numero == null || Numero == '') {
                Numero = '';
            }

            var pd = '';
            if (TipoCaracter == 'N') {
                pd = repitechar(TotalDigitos, '0');
            } else {
                pd = repitechar(TotalDigitos, ' ');
            }
            if (TipoCaracter == 'N') {
                Numero = pd.substring(0, mon_len) + Numero;
                return Numero;
            } else {
                Numero = Numero + pd;
                return Numero.substring(0, parseInt(TotalDigitos));
            }
        }


        function obtenerWithHoldingDescription(title, id) {

            var concept = '';
            if (title == 'National Tax') {
                var nationalTaxSearchObj = search.create({
                    type: "customrecord_lmry_national_taxes",
                    filters: [
                        ["internalid", "anyof", id]
                    ],
                    columns: [
                        search.createColumn({ name: "internalid", label: "Internal ID" }),
                        search.createColumn({ name: "custrecord_lmry_ntax_description", label: "Latam - Withholding Description" })
                    ]
                });
                var searchresult = nationalTaxSearchObj.run();
                var objResult = searchresult.getRange(0, 100);
                var columns = objResult[0].columns;

                concept = objResult[0].getValue(columns[1]);
                //  log.debug('concept',concept);


            } else if (title == 'Contributory Class') {
                var contributoryClassSearchObj = search.create({
                    type: "customrecord_lmry_ar_contrib_class",
                    filters: [
                        ["internalid", "anyof", id]
                    ],
                    columns: [
                        search.createColumn({ name: "internalid", label: "Internal ID" }),
                        search.createColumn({ name: "custrecord_lmry_ccl_description", label: "Latam - Withholding Description" })
                    ]
                });
                var searchresult = contributoryClassSearchObj.run();
                var objResult = searchresult.getRange(0, 100);
                var columns = objResult[0].columns;

                concept = objResult[0].getValue(columns[1]);
                //log.debug('concept',concept);
            } else {
                concept = '';
            }

            return concept;
        }


        function getTitle(parameTypeRet) {
            if (parameTypeRet == 1) {
                return GLOBAL_LABELS['tituloICA'][language];
            }

            if (parameTypeRet == 2) {
                return GLOBAL_LABELS['tituloRETE'][language];
            }

            if (parameTypeRet == 3) {
                return GLOBAL_LABELS['tituloIVA'][language];
            }
        }


        //-------------------------------------------------------------------------------------------------------
        //Replica un caracter segun la cantidad indicada
        //-------------------------------------------------------------------------------------------------------
        function repitechar(cantidad, carac) {
            var caracter = carac;
            var numero = parseInt(cantidad);
            var cadena = '';
            for (var r = 0; r < numero; r++) {
                cadena += caracter;
            }
            return cadena;
        }

        function calcular_tasa(tas_) {
            var tama = searchresultWhtCode_fin.length;

            for (var i = 0; i < tama; i++) {
                if (tas_ == searchresultWhtCode_fin[i].getValue(columnas_f[0])) {
                    return searchresultWhtCode_fin[i].getValue(columnas_f[2]);
                }
            }
        }

        function calcular_desc(des_) {
            var tama = searchresultWhtCode_fin.length;
            for (var i = 0; i < tama; i++) {
                if (des_ == searchresultWhtCode_fin[i].getValue(columnas_f[0])) {
                    return searchresultWhtCode_fin[i].getValue(columnas_f[1]);
                }
            }
        }


        function getVatRegistrationNo(paramsubsidi) {
            // Busqueda para obtener VAT REGISTRATION NO 
            var subsidiarySearchObj = search.lookupFields({
                type: search.Type.SUBSIDIARY,
                id: paramsubsidi,
                columns: ['taxidnum', 'custrecord_lmry_dig_verificador']
            });

            if (subsidiarySearchObj.custrecord_lmry_dig_verificador == '' || subsidiarySearchObj.custrecord_lmry_dig_verificador == null) {
                return subsidiarySearchObj.taxidnum;
            } else {
                return subsidiarySearchObj.taxidnum + '-' + subsidiarySearchObj.custrecord_lmry_dig_verificador;
            }
        }



        function getPeriods(paramperiodanio, paramSubsi) {
            var featCalendar = runtime.isFeatureInEffect({ feature: "MULTIPLECALENDARS" });
            var licenses = library.getLicenses(paramSubsi);
            featureSpecialPeriod = library.getAuthorization(677, licenses);
            var period = new Array();

            //Si tiene activado el Special Period
            if (featureSpecialPeriod == true || featureSpecialPeriod == 'T') {
                var varFilter = new Array();
                //Busqueda general con el paramperiod
                var varSpecialPeriod = search.create({
                    type: 'customrecord_lmry_special_accountperiod',
                    filters: [
                        ["custrecord_lmry_anio_fisco", "is", paramperiodanio]
                    ],
                    columns: ['custrecord_lmry_accounting_period', 'custrecord_lmry_anio_fisco']
                });
                //Filtro para probar si tiene multiples calendarios                            
                if (featCalendar == true || featCalendar == 'T') {
                    var searchSubsi = search.lookupFields({
                        type: 'subsidiary',
                        id: paramSubsi,
                        columns: ['fiscalcalendar']
                    });
                    var fiscalCalendar = searchSubsi.fiscalcalendar;
                    var jsonFiscalCalendar = JSON.stringify({ id: fiscalCalendar[0].value, nombre: fiscalCalendar[0].text });
                    varFilter = search.createFilter({
                        name: 'custrecord_lmry_calendar',
                        operator: search.Operator.IS,
                        values: jsonFiscalCalendar
                    });
                    // Agrega filtro del calendario de la subsidiaria
                    varSpecialPeriod.filters.push(varFilter);
                }
                // Ejecutando la busqueda
                var varResult = varSpecialPeriod.run();
                var varSpecialPeriodRpt = varResult.getRange({
                    start: 0,
                    end: 1000
                });
                if (varSpecialPeriodRpt == null || varSpecialPeriodRpt.length == 0) {
                    log.debug('NO DATA', 'No hay periodos para ese año seleccionado en la configuración del record LatamReady - Special Accounting Period');
                    return false;
                } else {
                    for (var i = 0; i < varSpecialPeriodRpt.length; i++) {
                        period[i] = new Array();
                        period[i] = varSpecialPeriodRpt[i].getValue('custrecord_lmry_accounting_period');
                    }
                    //log.debug('special accounting period', period);
                }
                return period;

            } else {
                var varFilter = new Array();
                // Busqueda para obtener AÑO Y MES (AAAAMM) 
                var periodSearchObj = search.lookupFields({
                    type: search.Type.ACCOUNTING_PERIOD,
                    id: paramperiodanio,
                    columns: ['startdate', 'enddate']
                });
                var periodStartDate = periodSearchObj.startdate;
                var periodEndDate = periodSearchObj.enddate;

                var accountingperiodObj = search.create({
                    type: "accountingperiod",
                    filters: [
                        ["isquarter", "is", "F"],
                        "AND", ["isyear", "is", "F"],
                        "AND", ["startdate", "onorafter", periodStartDate],
                        "AND", ["enddate", "onorbefore", periodEndDate]
                    ],
                    columns: [
                        search.createColumn({
                            name: "periodname",
                            sort: search.Sort.ASC,
                            label: "Name"
                        }),
                        search.createColumn({ name: "internalid", label: "Internal ID" }),
                        search.createColumn({ name: "startdate", label: "Start Date" }),
                        search.createColumn({ name: "enddate", label: "End Date" })
                    ]
                });
                //Filtro para probar si tiene multiples calendarios                            
                if (featCalendar == true || featCalendar == 'T') {
                    var varSubsidiary = search.lookupFields({
                        type: 'subsidiary',
                        id: paramSubsi,
                        columns: ['fiscalcalendar']
                    });
                    var fiscalCalendar = varSubsidiary.fiscalcalendar[0].value;
                    // log.debug('featCalendar',featCalendar);

                    varFilter = search.createFilter({
                        name: 'fiscalcalendar',
                        operator: search.Operator.IS,
                        values: fiscalCalendar
                    });
                    // Agrega filtro del calendario de la subsidiaria
                    accountingperiodObj.filters.push(varFilter);
                }
                // Ejecutando la busqueda
                var varResult = accountingperiodObj.run();
                var AccountingPeriodRpt = varResult.getRange({
                    start: 0,
                    end: 1000
                });
                if (AccountingPeriodRpt == null || AccountingPeriodRpt.length == 0) {
                    log.debug('NO DATA', 'No hay periodos para ese año seleccionado en la configuración del record LatamReady - Accounting Special Period');
                    return false;
                } else {
                    for (var i = 0; i < AccountingPeriodRpt.length; i++) {
                        period[i] = new Array();
                        period[i] = AccountingPeriodRpt[i].getValue('internalid');
                    }
                }
                log.debug('accounting arrperiod', period);
                return period;
            }
        }

        function generarStringFilterPostingPeriodAnual(idsPeriod) {
            var cant = idsPeriod.length;
            var comSimpl = "'";
            var strinic = "CASE WHEN ({postingperiod.id}=" + comSimpl + idsPeriod[0] + comSimpl;
            var strAdicionales = "";
            var strfinal = ") THEN 1 ELSE 0 END";
            for (var i = 1; i < cant; i++) {
                strAdicionales += " or {postingperiod.id}=" + comSimpl + idsPeriod[i] + comSimpl;
            }
            var str = strinic + strAdicionales + strfinal;
            //log.debug('periodsSTR', str);
            return str;
        }


        function logoSubsidiary(paramsubsidi) {
            subsi = recordModulo.load({
                type: "subsidiary",
                id: paramsubsidi
            })

            //logo
            subsi_log = subsi.getValue('logo');
            if (subsi_log) {
                var _logo = fileModulo.load({
                    id: subsi_log
                })
                subsi_logo = _logo.url;


                var host = url.resolveDomain({
                    hostType: url.HostType.APPLICATION,
                    accountId: runtime.accountId
                });

                return subsi_logo = "https://" + host + subsi_logo;
            }
        }


        function ObtenerParametrosYFeatures() {

            columnas_f[0] = search.createColumn({
                name: 'name',
                label: 'Name'
            });

            columnas_f[1] = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_wht_codedesc}',
                label: "WHT-CODEDESC"
            });

            columnas_f[2] = search.createColumn({
                name: "formulatext",
                formula: "{custrecord_lmry_wht_coderate}",
                label: "WHT-CODERATE"
            });

            result_wht_code = search.create({
                type: "customrecord_lmry_wht_code",
                filters: [],
                columns: columnas_f
            });
            var searchresultWhtCode = result_wht_code.run();
            searchresultWhtCode_fin = searchresultWhtCode.getRange(0, 1000);

            //Parametros
            var objContext = runtime.getCurrentScript();

            paramsubsidi = objContext.getParameter({
                name: 'custscript_lmry_co_subsi_withbk_ret_acum'
            });
            nameDIAN = objContext.getParameter({
                name: 'custscript_lmry_co_dian_name'
            });
            if (nameDIAN == null || nameDIAN == "- None -" || nameDIAN == "") {
                nameDIAN = ' ';
            }

            paramperiodanio = objContext.getParameter({
                name: 'custscript_lmry_co_par_anio_wtbk_ret_ac'
            });


            calcularAnio();

            paramMulti = objContext.getParameter({
                name: 'custscript_lmry_co_multibook_wtbk_ret_ac'
            });

            paramidrpt = objContext.getParameter({
                name: 'custscript_lmry_co_idrpt_wtbk_ret_acumul'
            });

            paramVendor = objContext.getParameter({
                name: 'custscript_lmry_co_vendor_withbk_ret_ac'
            });

            paramTyreten = objContext.getParameter({
                name: 'custscript_lmry_co_type_withbk_ret_acum'
            });

            paramCont = objContext.getParameter({
                name: 'custscript_lmry_co_cont_withbk_ret_acum'
            });

            paramBucle = objContext.getParameter({
                name: 'custscript_lmry_co_bucle_withbk_ret_acum'
            });

            paramGroupingMonth = objContext.getParameter({
                name: 'custscript_lmry_co_group_month'
            });

            if (paramCont == null) {
                paramCont = 0;
            }

            if (paramBucle == null) {
                paramBucle = 0;
            }

            //Features
            featSubsi = runtime.isFeatureInEffect({
                feature: "SUBSIDIARIES"
            });
            featMulti = runtime.isFeatureInEffect({
                feature: "MULTIBOOK"
            });

            log.error({
                title: 'ENTROfeats',
                details: paramidrpt + ' ' + paramMulti + ' ' + paramsubsidi + ' ' + paramperiodanio + ' ' + paramVendor + ' ' + paramTyreten
            });

            log.error({
                title: 'otrosparametros',
                details: nameDIAN + ' ' + paramCont + ' ' + paramBucle + ' - ' + paramGroupingMonth
            });

            // Para buscar la municipality
            log.error('paramVendor', paramVendor);

            if (paramTyreten == 1) {
                municipality = getMunicipalityByVendorICA(paramVendor) || getMunicipalityBySubsidiary();
                municipality = municipality || 'BOGOTA';
            } else {
                municipality = getMunicipalityByVendorOthers(paramVendor) || 'BOGOTA';
            }
            //Multibook Name
            if (featMulti) {
                var multibookName_temp = search.lookupFields({
                    type: search.Type.ACCOUNTING_BOOK,
                    id: paramMulti,
                    columns: ['name']
                });

                multibookName = multibookName_temp.name;
                log.error({
                    title: 'MULTIBOOK',
                    details: multibookName
                });
            }

            var result_f_temp = search.create({
                type: search.Type.CURRENCY,
                columns: ['name', 'symbol']
            });
            var result_f_temp2 = result_f_temp.run();
            result_f = result_f_temp2.getRange(0, 1000);
        }

        function getNameSubsidiaria(municipality_id) {

            var municipalidad = '';

            if (municipality_id != '' && municipality_id != null) {

                var municipality_Temp = search.lookupFields({
                    type: 'customrecord_lmry_co_entitymunicipality',
                    id: municipality_id,
                    columns: ['custrecord_lmry_co_municcode']
                });

                var code_municipality = municipality_Temp.custrecord_lmry_co_municcode;

                var searchCity = search.create({
                    type: "customrecord_lmry_city",
                    filters: [
                        ["custrecord_lmry_city_country", "anyof", "48"],
                        "AND", ["custrecord_lmry_city_id", "is", code_municipality]
                    ],
                    columns: [
                        search.createColumn({
                            name: "name",
                        })
                    ]
                });

                var resultObj = searchCity.run();
                var searchResultArray = resultObj.getRange(0, 1000);

                if (searchResultArray != null && searchResultArray.length != 0) {
                    municipalidad = searchResultArray[0].getValue("name");
                    if (municipalidad != '' && municipalidad != null) {
                        municipalidad = municipalidad.replace('BOGOTA BOGOTA, D.C.', 'BOGOTA');
                    }
                }
            }

            return municipalidad;
        }

        function getMunicipalityBySubsidiary() {

            var municipalidad = '';

            if (paramsubsidi != '' && paramsubsidi != null) {
                var municipality_id_Temp = search.lookupFields({
                    type: search.Type.SUBSIDIARY,
                    id: paramsubsidi,
                    columns: ['custrecord_lmry_municipality_sub']
                });

                if (municipality_id_Temp.custrecord_lmry_municipality_sub.length != 0) {
                    var municipality_id = municipality_id_Temp.custrecord_lmry_municipality_sub[0].value;
                }
                if (municipality_id != '' && municipality_id != null) {
                    municipalidad = getNameSubsidiaria(municipality_id);
                }
            }
            //log.debug('municipality by Subsidiaria:', municipalidad);
            return municipalidad;
        }

        function getMunicipalityByVendorICA(idvendor) {

            var municipalidad = '';

            if (idvendor != '' && idvendor != null) {

                var vendorTemp = search.lookupFields({
                    type: search.Type.VENDOR,
                    id: idvendor,
                    columns: ['custentity_lmry_municipality']
                });

                if (vendorTemp.custentity_lmry_municipality.length != 0) {
                    var municipality_id = vendorTemp.custentity_lmry_municipality[0].value;
                }

                municipalidad = getNameSubsidiaria(municipality_id);
            }
            //log.debug('municipality by vendor:', municipalidad);
            return municipalidad;

        }

        function getMunicipalityByVendorOthers(idvendor) {
            var municipalidad = '';

            if (idvendor != '' && idvendor != null) {

                var vendorSearchObj = search.create({
                    type: "vendor",
                    filters: [
                        ["internalid", "anyof", idvendor]
                    ],
                    columns: [
                        search.createColumn({
                            name: "custrecord_lmry_addr_city",
                            join: "Address",
                            label: "Latam - City"
                        })
                    ]
                });
                var objResult = vendorSearchObj.run().getRange(0, 1000);
                if (objResult && objResult.length) {
                    var columns = objResult[0].columns;
                    municipalidad = objResult[0].getText(columns[0]);
                }

            }
            //log.debug('municipality by vendor:', municipalidad);
            return municipalidad;
        }

        function obtenerMeses() {


            log.debug('obtenerMeses - ANIO', anio);

            var periodStartDate = format.format({
                value: new Date(anio, 0, 1),
                type: format.Type.DATE
            });

            var periodEndDate = format.format({
                value: new Date(anio, 11, 31),
                type: format.Type.DATE
            });

            var accountingperiodSearchObj = search.create({
                type: "accountingperiod",
                filters: [
                    ["startdate", "onorafter", periodStartDate],
                    "AND", ["enddate", "onorbefore", periodEndDate],
                    "AND", ["isquarter", "is", "F"],
                    "AND", ["isyear", "is", "F"]
                ],
                columns: [
                    search.createColumn({ name: "internalid", label: "Internal ID" }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "CASE  WHEN EXTRACT(Month FROM {enddate})= 1 THEN 'Enero'  WHEN EXTRACT(Month FROM {enddate})= 2 THEN 'Febrero' WHEN EXTRACT(Month FROM {enddate})= 3 THEN 'Marzo' WHEN EXTRACT(Month FROM {enddate})= 4 THEN 'Abril' WHEN EXTRACT(Month FROM {enddate})= 5 THEN 'Mayo' WHEN EXTRACT(Month FROM {enddate})= 6 THEN 'Junio' WHEN EXTRACT(Month FROM {enddate})= 7 THEN 'Julio' WHEN EXTRACT(Month FROM {enddate})= 8 THEN 'Agosto' WHEN EXTRACT(Month FROM {enddate})= 9 THEN 'Septiembre' WHEN EXTRACT(Month FROM {enddate})= 10 THEN 'Octubre' WHEN EXTRACT(Month FROM {enddate})= 11 THEN 'Noviembre' WHEN EXTRACT(Month FROM {enddate})= 12 THEN 'Diciembre' ELSE 'PERIODO DE AJUSTE' END",
                        label: "Formula (Text)"
                    })
                ]
            });

            var pagedData = accountingperiodSearchObj.runPaged({
                pageSize: 1000
            });

            var page, idPeriod, mes;
            var jsonMeses = {};

            pagedData.pageRanges.forEach(function(pageRange) {

                page = pagedData.fetch({
                    index: pageRange.index
                });
                page.data.forEach(function(result) {

                    idPeriod = '';
                    mes = '';

                    // 0. ID PERIODO
                    if (result.getValue(result.columns[0]) != '- None -' && result.getValue(result.columns[0]) != '') {
                        idPeriod = result.getValue(result.columns[0]);
                    } else {
                        idPeriod = '';
                    }

                    // 1. MES (ENERO - DICIEMBRE)
                    if (result.getValue(result.columns[1]) != '- None -' && result.getValue(result.columns[1]) != '') {
                        mes = result.getValue(result.columns[1]);
                    } else {
                        mes = '';
                    }


                    jsonMeses[idPeriod] = mes;
                });
            });

            log.debug('jsonMeses', jsonMeses);

            return jsonMeses;
        }

        function getGlobalLabels() {

            var labels = {
                "tituloICA": {
                    "es": 'CERTIFICADO DE RETENCIÓN DEL IMPUESTO DE INDUSTRIA Y COMERCIO',
                    "pt": 'CERTIFICADO DE RETENÇÃO DE IMPOSTO DE INDÚSTRIA E COMÉRCIO',
                    "en": 'INDUSTRY AND COMMERCE TAX WITHHOLDING CERTIFICATE'
                },
                "tituloRETE": {
                    "es": 'CERTIFICADO DE RETENCIÓN EN LA FUENTE',
                    "pt": 'CERTIFICADO DE RETENÇÃO NA FONTE',
                    "en": 'CERTIFICATE OF WITHHOLDING AT THE SOURCE'
                },
                "tituloIVA": {
                    "es": 'CERTIFICADO DE RETENCIÓN EN LA FUENTE POR IVA',
                    "pt": 'CERTIFICADO DE RETENÇÃO DE IVA',
                    "en": 'VAT WITHHOLDING CERTIFICATE'
                },
                "AnioGravable": {
                    "es": 'AÑO GRAVABLE ',
                    "pt": 'ANO TRIBUTÁVEL ',
                    "en": 'TAXABLE YEAR '
                },
                "Articulo381_ICA": {
                    "es": 'Para dar Cumplimiento al artículo 381 del Estatuto tributario, certificamos que practicamos retenciones a título de ICA. ',
                    "pt": 'Para cumprimento do artigo 381.º do Estatuto Fiscal, certificamos que efetuamos retenções como ICA. ',
                    "en": 'To comply with article 381 of the Tax Statute, we certify that we make withholdings as ICA. '
                },
                "Articulo381_RENTA": {
                    "es": 'Para dar Cumplimiento al artículo 381 del Estatuto tributario, certificamos que practicamos retenciones a título de RENTA. ',
                    "pt": 'Para cumprimento do artigo 381.º do Estatuto Fiscal, certificamos que efetuamos retenções para ALUGUEL. ',
                    "en": 'To comply with article 381 of the Tax Statute, we certify that we make withholdings for RENT. '
                },
                "Articulo381_IVA": {
                    "es": 'Para dar Cumplimiento al artículo 381 del Estatuto tributario, certificamos que practicamos retenciones a título de IVA. ',
                    "pt": 'Para cumprimento do artigo 381.º do Estatuto Fiscal, certificamos que efetuamos retenções de IVA. ',
                    "en": 'To comply with article 381 of the Tax Statute, we certify that we make VAT withholdings. '
                },
                'MES': {
                    "es": 'MES',
                    "pt": 'MES',
                    "en": 'MONTH'
                },
                'CONCEPTO': {
                    "es": 'CONCEPTO',
                    "pt": 'CONCEITO',
                    "en": 'CONCEPT'
                },
                'BASE': {
                    "es": 'BASE',
                    "pt": 'BASE DE',
                    "en": 'RETENTION'
                },
                'RETENCION': {
                    "es": 'RETENCION',
                    "pt": 'RETENÇÃO',
                    "en": 'BASE'
                },
                'PORC': {
                    "es": 'PORC.',
                    "pt": 'PERC.',
                    "en": 'PERC.'
                },
                'VALOR': {
                    "es": 'VALOR',
                    "pt": 'VALOR',
                    "en": 'RETAINED'
                },
                'RETENIDO': {
                    "es": 'RETENIDO',
                    "pt": 'RETIDO',
                    "en": 'VALUE'
                },
                'RETEICA_PIE': {
                    "es": 'Los valores retenidos fueron consignados oportunamente en la Ciudad de ',
                    "pt": 'Os valores retidos foram oportunamente consignados na Cidade de ',
                    "en": 'The amounts withheld were timely consigned in the City of '
                },
                'PIE': {
                    "es": 'Los valores retenidos fueron consignados oportunamente a favor de la DIRECCION DE IMPUESTOS Y ADUANAS NACIONALES DIAN en la Ciudad de ',
                    "pt": 'Os valores retidos foram oportunamente consignados em favor da DIRETORIA NACIONAL TRIBUTÁRIA E ADUANEIRA DIAN na Cidade de ',
                    "en": 'The amounts withheld were timely consigned in favor of the NATIONAL TAX AND CUSTOMS DIRECTORATE DIAN in the City of '
                },
                'FirmaAutografaReteICA': {
                    "es": 'Este documento no requiere para su validez firma autógrafa de acuerdo con el artículo 10 del Decreto 836 de 1991, recopilado en el artículo 1.6.1.12.12 del DUT 1625 de octubre de 2016, que regula el contenido del certificado de retenciones a título de renta.',
                    "pt": 'Este documento dispensa assinatura de autógrafo para sua validade, conforme artigo 10 do Decreto 836 de 1991, compilado no artigo 1.6.1.12.12 do DUT 1625 de outubro de 2016, que regulamenta o conteúdo do certificado de retenção de rendimentos.',
                    "en": 'This document does not require an autograph signature for its validity in accordance with article 10 of Decree 836 of 1991, compiled in article 1.6.1.12.12 of DUT 1625 of October 2016, which regulates the content of the withholding certificate for income.'
                },
                'FirmaAutografaReteFTE': {
                    "es": 'Este documento no requiere para su validez firma autógrafa de acuerdo con el artículo 10 del Decreto 836 de 1991, recopilado en el artículo 1.6.1.12.12 del DUT 1625 de octubre de 2016, que regula el contenido del certificado de retenciones a título de renta.',
                    "pt": 'Este documento dispensa assinatura de autógrafo para sua validade, conforme artigo 10 do Decreto 836 de 1991, compilado no artigo 1.6.1.12.12 do DUT 1625 de outubro de 2016, que regulamenta o conteúdo do certificado de retenção de rendimentos.',
                    "en": 'This document does not require an autograph signature for its validity in accordance with article 10 of Decree 836 of 1991, compiled in article 1.6.1.12.12 of DUT 1625 of October 2016, which regulates the content of the withholding certificate for income.'
                },
                'FirmaAutografaReteIVA': {
                    "es": 'Este documento no requiere para su validez firma autógrafa de acuerdo con el artículo 7 del Decreto 380 de 1996, recopilado en el artículo 1.6.1.12.13 del DUT 1625 de octubre de 2016, que regula el contenido del certificado de retenciones a título de IVA.',
                    "pt": 'Este documento não carece de assinatura autógrafo para a sua validade nos termos do artigo 7.º do Decreto 380 de 1996, compilado no artigo 1.6.1.12.13 do DUT 1625 de outubro de 2016, que regulamenta o conteúdo do certificado de retenção de IVA.',
                    "en": 'This document does not require an autograph signature for its validity in accordance with article 7 of Decree 380 of 1996, compiled in article 1.6.1.12.13 of DUT 1625 of October 2016, which regulates the content of the VAT withholding certificate.'
                },
                'DomicilioPrincipal': {
                    "es": 'DOMICILIO PRINCIPAL: ',
                    "pt": 'RESIDÊNCIA PRIMÁRIA: ',
                    "en": 'PRIMARY RESIDENCE: '
                },
                'FechaExpedicion': {
                    "es": 'FECHA DE EXPEDICION: ',
                    "pt": 'DATA DE EXPEDIÇÃO: ',
                    "en": 'EXPEDITION DATE: '
                },
                'AgenteRetenedor': {
                    "es": 'Agente Retenedor: ',
                    "pt": 'Agente de retenção: ',
                    "en": 'Withholding Agent: '
                },
                'NITCédula': {
                    "es": 'NIT o Cédula: ',
                    "pt": 'NIT ou ID: ',
                    "en": 'NIT or ID: '
                },
                'Direccion': {
                    "es": 'Dirección: ',
                    "pt": 'Direção: ',
                    "en": 'Direction: '
                },
                'Ciudad': {
                    "es": 'Ciudad: ',
                    "pt": 'Cidade: ',
                    "en": 'City: '
                },
                'PagadoA': {
                    "es": 'Pagado a:  ',
                    "pt": 'Paga para: ',
                    "en": 'Paid to: '
                },
                'NIT/Cédula': {
                    "es": 'NIT/Cédula:  ',
                    "pt": 'NIT/ID: ',
                    "en": 'NIT/ID: '
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
            }
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
        function getTimeHardcoded(datetime) {
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