/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_CertifiRetenciones_SCHDL_v2.0.js        ||
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
        "N/format", "N/log", "N/config", "N/sftp", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js", "N/task", "N/render", "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js", 'N/xml'
    ],

    function(recordModulo, runtime, fileModulo, email, encode, search, format, log,
        config, sftp, libreria, task, render, libreriaGeneral, xml) {
        var objContext = runtime.getCurrentScript();
        var language = objContext.getParameter("LANGUAGE").substring(0, 2);
        //Tamaño
        var file_size = 7340032;

        // Nombre del Reporte
        var namereport = "Reporte de Certificado de Retenciones";
        var LMRY_script = 'LMRY CO Reportes Certificado de Retencion SCHDL 2.0';

        //Parametros
        var paramsubsidi = '';
        var paramperiodoInicio = '';
        var paramperiodoFinal = '';
        var paramVendor = '';
        var paramTyreten = '';
        var paramidrpt = '';
        var paramMulti = '';
        var paramCont = '';
        var paramBucle = '';

        //Control de Reporte
        var periodstartdate = '';
        var periodenddate = '';
        var antperiodenddate = '';
        var companyruc = '';
        var companyname = '';
        var companyaddress = '';
        var companyname_vendor = '';
        var nit_vendor = '';

        var fechalog = '';
        var ArrReteAux = new Array();
        var ArrRetencion = new Array();

        var arrNationalTaxIds = new Array();
        var arrContriClassIds = new Array();

        var jsonTransactionMunicip = {};

        var strName = '';
        var strNameFile = '';
        var periodname = '';
        var auxmess = '';
        var auxanio = '';
        var strConcepto = '';
        var Final_string;
        var columnas_f = new Array();
        var ExchangerateC_S;
        var Inicio = '';
        var Final = '';
        var Inicio_Fecha = '';
        var Final_Fecha = '';
        var num_muni = 0;
        var name_muni = '';

        var multibook = '';
        var GLOBAL_LABELS = {};

        /******************************************
         * @leny - Modificado el 28/08/2015
         * Nota: Variables para acumulacions de Montos.
         ******************************************/
        var montototal;
        var montoBase = 0;
        var nameDIAN = '';
        var municipality = '';

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

        //PDF Normalization
        var todays = "";
        var currentTime = "";

        function execute(context) {
            GLOBAL_LABELS = getGlobalLabels();
            try {
                ObtenerParametrosYFeatures();
                ObtenerDatosSubsidiaria();
                obtenerFechaFinal();

                if (paramTyreten == 1) {
                    ArrRetencion = ObtieneRetencionReteICA();
                } else if (paramTyreten == 2) {
                    ArrRetencion = ObtieneRetencionReteFTE();
                } else if (paramTyreten == 3) {
                    ArrRetencion = ObtieneRetencionReteIVA();
                }

                todays = parseDateTo(new Date(), "DATE");
                currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

                if (ArrRetencion.length != 0) {
                    if (paramTyreten == 1) {
                        jsonTransactionMunicip = obtenerTransaccionesXMunicipalidad(ArrRetencion);

                        for (key in jsonTransactionMunicip) {
                            
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

            } catch (err) {
                libreriaGeneral.sendErrorEmail(err, LMRY_script, language);
                //libreria.sendMail(LMRY_script, ' [ execute ] ' + err);
                //var varMsgError = 'No se pudo procesar el Schedule.';
            }
        }

        function obtenerTransaccionesXMunicipalidad(ArrRetencion) {
            var jsonAgrupadoxMun = {};
            for (var i = 0; i < ArrRetencion.length; i++) {
                var municipalidad = (getNameSubsidiaria(ArrRetencion[i][11]) || municipality);

                if (jsonAgrupadoxMun[municipalidad] != undefined) {
                    jsonAgrupadoxMun[municipalidad].push(ArrRetencion[i]);
                } else {
                    jsonAgrupadoxMun[municipalidad] = [ArrRetencion[i]]
                }
            }
            return jsonAgrupadoxMun;
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
            if (splitLeft.charAt(0)==='-') {
                splitLeft=splitLeft.slice(1)
                pSimbolo='-'+pSimbolo
            }
            var valor = pSimbolo + splitLeft + splitRight;
            return valor;
        }

        //-------------------------------------------------------------------------------------------------------
        //Generaci?n Detalle Retencion en PDF
        //-------------------------------------------------------------------------------------------------------
        function DetalleRetencion() {
            var strAux = '';

            for (var i = 0; i <= ArrRetencion.length - 1; i++) {
                /******************************************
                 * @leny - Modificado el 28/08/2015
                 * Nota: Se acumulacion de montos.
                 ******************************************/
                
                if (ArrRetencion[i][3]=='VendBill') {
                    ArrRetencion[i][6]=Math.abs(ArrRetencion[i][6]);
                    ArrRetencion[i][7]=Math.abs(ArrRetencion[i][7]);
                }else if (ArrRetencion[i][3]=='VendCred') {
                    ArrRetencion[i][6]=-Math.abs(ArrRetencion[i][6]);
                    ArrRetencion[i][7]=-Math.abs(ArrRetencion[i][7]);
                }
                montoBase = parseFloat(montoBase) + parseFloat(ArrRetencion[i][6]);
                montototal = parseFloat(montototal) + parseFloat(Math.round(parseFloat(Number(ArrRetencion[i][7])) * 100) / 100);
                //montototal = parseFloat(montototal) + parseFloat(ArrRetencion[i][7]);
                // montototal = parseFloat(montototal) + parseFloat(Math.round(parseFloat(Number(ArrRetencion[i][7])) * 100) / 100);

                strAux += "<tr>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\">";
                strAux += "<p>" + xml.escape(ArrRetencion[i][10]) + "</p>";
                strAux += "</td>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\">";
                strAux += "<p>" + xml.escape(ArrRetencion[i][4]) + "</p>";
                strAux += "</td>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                strAux += "<p>" + FormatoNumero(parseFloat(ArrRetencion[i][6]).toFixed(2), "$") + "</p>";
                strAux += "</td>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                strAux += "<p>" + ArrRetencion[i][9] + "%</p>";
                strAux += "</td>";
                strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
                strAux += "<p>" + FormatoNumero(parseFloat(ArrRetencion[i][7]).toFixed(2), "$") + "</p>";
                strAux += "</td>";
                strAux += "</tr>";

            }

            return strAux;
        }

        function Name_File() {
            //AR_RETE_PERC_IIBB_CABA_XXXXXX_MMYYYY_S_T_M_C.txt
            var _NameFile = '';

            var fecha_format = format.parse({
                value: paramperiodoInicio,
                type: format.Type.DATE
            });

            var MM = fecha_format.getMonth() + 1;
            var YYYY = fecha_format.getFullYear();
            var DD = fecha_format.getDate();

            if (('' + MM).length == 1) {
                MM = '0' + MM;
            }
            Inicio = Periodo(MM) + ' ' + YYYY;


            var fecha_format = format.parse({
                value: paramperiodoFinal,
                type: format.Type.DATE
            });

            var MM = fecha_format.getMonth() + 1;
            var YYYY = fecha_format.getFullYear();
            var DD = fecha_format.getDate();

            if (('' + MM).length == 1) {
                MM = '0' + MM;
            }

            Final = Periodo(MM) + ' ' + YYYY;

            _NameFile = strNameFile + '_' + companyname + '_' + companyname_vendor + '_' + Inicio + '_' + Final;
            

            return _NameFile;

        }

        //-------------------------------------------------------------------------------------------------------
        // Graba el archivo en el Gabinete de Archivos
        //-------------------------------------------------------------------------------------------------------
        function SaveFile() {
           

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
                        NameFile = Name_File() + '_' + name_muni + '_' + paramMulti + '_' + fileext;
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

                if (getURL != '' && getURL != '') {
                    urlfile += 'https://' + getURL;
                }
                urlfile += idfile2.url;

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

                        //Periodo
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_postingperiod',
                            value: Inicio
                        });

                        //Nombre de Reporte
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_transaction',
                            value: 'CO - Certificado de Retención v2018.2'
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
                        //libreria.sendrptuser('CO - Certificado de Retención v2018.2', 3, NameFile);
                        libreriaGeneral.sendConfirmUserEmail('CO - Certificado de Retención v2018.2', 3, NameFile, language);
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

                        //Periodo
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_postingperiod',
                            value: Inicio
                        });

                        //Nombre de Reporte
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_transaction',
                            value: 'CO - Certificado de Retención v2018.2'
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
                        //libreria.sendrptuser('CO - Certificado de Retención v2018.2', 3, NameFile);
                        libreriaGeneral.sendConfirmUserEmail('CO - Certificado de Retención v2018.2', 3, NameFile, language);
                    }
                    num_muni++;
                }
            } else {
                // Debug
                log.debug({
                    title: 'DEBUG',
                    details: 'Creacion de Txt' +
                        'No se existe el folder'
                });
            }
        }

        function RecordNoData() {
            

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
                value: GLOBAL_LABELS['nodata'][language]
            });

            //Nombre de Reporte
            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_transaction',
                value: 'CO - Certificado de Retención v2018.2'
            });

            //Nombre de Subsidiaria
            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_subsidiary',
                value: companyname
            });

            //Periodo
            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_postingperiod',
                value: Inicio
            });

            //Multibook
            if (featMulti) {
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
                            nit_vendor = columnFrom2 + "-" + " ";
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

        function obtenerFechaFinal() {

            // Declaracion de variables

            var fecha_format = format.parse({
                value: paramperiodoInicio,
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
                value: paramperiodoFinal,
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

        }


        //-------------------------------------------------------------------------------------------------------
        //Generaci?n archivo PDF
        //-------------------------------------------------------------------------------------------------------
        function GeneracionPDF() {
            var strName = '';
            montototal = 0;
            ObtainVendor(paramVendor);

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

            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\" align=\"center\">";
            strHead += GLOBAL_LABELS['origin'][language] + " Netsuite";
            strHead += "</td>";
            strHead += "</tr>";
            
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\" align=\"center\">";
            strHead += GLOBAL_LABELS['date'][language] + " " + todays;
            strHead += "</td>";
            strHead += "</tr>";
            
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\" align=\"center\">";
            strHead += GLOBAL_LABELS['time'][language] + " " + currentTime;
            strHead += "</td>";
            strHead += "</tr>";

            strHead += "</table>";
            strHead += "<p></p>";

            strHead += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 16pt; border: 0px solid #000000\" align=\"center\">";
            strHead += "<p>" + GLOBAL_LABELS['titulo'][language] + "</p>";
            strHead += "</td>";
            strHead += "</tr>";



            // Impuesto ICA

            if (paramTyreten == 1) {
                strHead += "<tr>";
                strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
                strHead += GLOBAL_LABELS['articulo381'][language];
                strHead += GLOBAL_LABELS['entre_inicio'][language] + Inicio_Fecha + GLOBAL_LABELS['entre_final'][language] + Final_Fecha + GLOBAL_LABELS['practicaIca'][language];
                strHead += "</td>";
                strHead += "</tr>";
            }
            // Impuesto RENTA
            if (paramTyreten == 2) {
                strHead += "<tr>";
                strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
                strHead += GLOBAL_LABELS['articulo381'][language];
                strHead += GLOBAL_LABELS['entre_inicio'][language] + Inicio_Fecha + GLOBAL_LABELS['entre_final'][language] + Final_Fecha + GLOBAL_LABELS['practicaRenta'][language];
                strHead += "</td>";
                strHead += "</tr>";
            }
            // Impuesto IVA
            if (paramTyreten == 3) {
                strHead += "<tr>";
                strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
                strHead += GLOBAL_LABELS['articulo381'][language];
                strHead += GLOBAL_LABELS['entre_inicio'][language] + Inicio_Fecha + GLOBAL_LABELS['entre_final'][language] + Final_Fecha + GLOBAL_LABELS['practicaIva'][language];
                strHead += "</td>";
                strHead += "</tr>";
            }

            strHead += "</table>";

            strHead += "<p></p>";
            strHead += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strHead += "<tr>";
            strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strHead += "<p>" + xml.escape(companyname_vendor) + "</p>";
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
            strDeta += "<thead>";
            strDeta += "<tr>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"25mm\">";
            strDeta += "<p>" + GLOBAL_LABELS['concepto'][language] + "</p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"65mm\">";
            strDeta += "<p>" + GLOBAL_LABELS['nfactura'][language] + "</p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
            strDeta += GLOBAL_LABELS['base'][language] + "<br/>";
            strDeta += GLOBAL_LABELS['retencion'][language];
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"30mm\">";
            strDeta += "<p>" + GLOBAL_LABELS['porc'][language] + "</p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
            strDeta += GLOBAL_LABELS['valor'][language] + "<br/>";
            strDeta += GLOBAL_LABELS['retenido'][language];
            strDeta += "</td>";
            strDeta += "</tr>";
            strDeta += "</thead>";

            if (paramTyreten == 1) {
                strNameFile = 'COCertificadoReteICA';
                strConcepto = 'Retencion ICA';
            }
            // ReteFte
            if (paramTyreten == 2) {
                strNameFile = 'COCertificadoReteFte';
                strConcepto = 'Retencion en la Fuente';
            }
            // ReteIVA
            if (paramTyreten == 3) {
                strNameFile = 'COCertificadoReteIVA';
                strConcepto = 'Retencion IVA';
            }

            strDeta += "<tbody>";
            strDeta += DetalleRetencion();

            /******************************************
             * @leny - Modificado el 27/08/2015
             * Nota: Se esta agregando la linia totales.
             ******************************************/
            strDeta += "<tr>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"50mm\">";
            strDeta += "<p>TOTAL</p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"35mm\">";
            strDeta += "<p></p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"right\" width=\"45mm\">";
            strDeta += "<p>" + FormatoNumero(parseFloat(montoBase).toFixed(2), "$") + "</p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"22mm\">";
            strDeta += "<p></p>";
            strDeta += "</td>";
            strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"right\" width=\"42mm\">";
            strDeta += "<p>" + FormatoNumero(parseFloat(montototal).toFixed(2), "$") + "</p>";
            strDeta += "</td>";
            strDeta += "</tr>";

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
                strNpie += GLOBAL_LABELS['pieReteIca1'][language] + municipality + ".";
                strNpie += "</td>";
                strNpie += "</tr>";
                strNpie += "</table>";
            } else {
                strNpie += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
                strNpie += "<tr>";
                strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
                strNpie += GLOBAL_LABELS['pieReteOtros1'][language] + municipality + ".";
                strNpie += "</td>";
                strNpie += "</tr>";
                strNpie += "</table>";
            }
            strNpie += "<p></p>";
            strNpie += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strNpie += GLOBAL_LABELS['firma'][language];
            strNpie += "</td>";
            strNpie += "</tr>";
            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strNpie += "(ART .10 D.R. 836/91)";
            strNpie += "</td>";
            strNpie += "</tr>";
            var auxDireccion = '';
            if (companyaddress != '') {
                var auxStr = companyaddress.split('\n');
                auxDireccion = auxStr[1];
                //companyaddress = 'xxxx';
            }
            var fecha_actual = new Date();
            fecha_actual = fecha_actual.getDate() + "/" + (fecha_actual.getMonth() + 1) + "/" + fecha_actual.getFullYear();

            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strNpie += GLOBAL_LABELS['domicilio'][language] + xml.escape(companyaddress);
            strNpie += "</td>";
            strNpie += "</tr>";
            strNpie += "<tr>";
            strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
            strNpie += GLOBAL_LABELS['fechaExpedicion'][language] + fecha_actual;
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

            var _cont = 0;
            var intDMinReg = Number(paramBucle) * 1000;
            var intDMaxReg = intDMinReg + 1000;
            var DbolStop = false;
            var infoTxt = '';
            var arrAuxiliar = new Array();

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

            var periodFilterIni = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORAFTER,
                values: [paramperiodoInicio]
            });
            saved_search.filters.push(periodFilterIni);

            var periodFilterFin = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORBEFORE,
                values: [paramperiodoFinal]
            });
            saved_search.filters.push(periodFilterFin);

            var entityFilter = search.createFilter({
                name: 'name',
                operator: search.Operator.IS,
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
            //11.- Campo del LATAM - BASE AMOUNT LOCAL CURRENCY
            var montoBaseTaxResult = search.createColumn({
                name: 'formulacurrency',
                formula: '{custrecord_lmry_br_transaction.custrecord_lmry_base_amount_local_currc}',
                label: '11. BASE AMOUNT LOCAL CURRENCY'
            });
            saved_search.columns.push(montoBaseTaxResult);
            //12.-Campo del LATAM - AMOUNT LOCAL CURRENCY
            var montoLocalTaxResult = search.createColumn({
                name: 'formulacurrency',
                formula: '{custrecord_lmry_br_transaction.custrecord_lmry_amount_local_currency}',
                label: '12. AMOUNT LOCAL CURRENCY'
            });
            saved_search.columns.push(montoLocalTaxResult);
            //13.-Campo de description
            var descriptionTaxResult = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_br_transaction.custrecord_lmry_tax_description}',
                label: '13. Description'
            });
            saved_search.columns.push(descriptionTaxResult);

            //14.-Municipalidad
            var municipTransaction = search.createColumn({
                name: "internalid",
                join: "CUSTBODY_LMRY_MUNICIPALITY",
                label: "14. Municipality"
            });
            saved_search.columns.push(municipTransaction);

            var jsonNT = {};
            var jsonCC = {};

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

                        arrAuxiliar = new Array();
                        // 0. C?DIGO WHT
                        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
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

                        // exchange rate cabecera y multibook

                        if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != '') {
                            var ExchangerateAux = objResult[i].getValue(columns[10]);
                            ExchangerateC_S = exchange_rate(ExchangerateAux);
                        } else {
                            ExchangerateC_S = 1;
                        }

                        // 6. BASE IMPONIBLE
                        if (objResult[i].getValue(columns[11]) && objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 0) {
                            arrAuxiliar[6] = objResult[i].getValue(columns[11]);
                        } else {
                            if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                arrAuxiliar[6] = (parseFloat(objResult[i].getValue(columns[6]))) * (Number(ExchangerateC_S));
                            } else
                                arrAuxiliar[6] = '0.00';
                        }

                        // 7. RETENCION
                        if (objResult[i].getValue(columns[12]) && objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 0) {
                            arrAuxiliar[7] = objResult[i].getValue(columns[12]);
                        } else {
                            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {
                                arrAuxiliar[7] = (parseFloat(objResult[i].getValue(columns[7]))) * (Number(ExchangerateC_S));
                            } else
                                arrAuxiliar[7] = '0.00';
                        }

                        //8. FECHA
                        if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -') {
                            arrAuxiliar[8] = objResult[i].getValue(columns[8]);
                        } else
                            arrAuxiliar[8] = '';

                        //9. DESCRIPCION
                        if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -') {
                            arrAuxiliar[9] = Number(objResult[i].getValue(columns[9])).toFixed(2);
                        } else
                            arrAuxiliar[9] = '';

                        //10. DESCRIPCION NATIONAL TAX O CONTRIBUCION
                        if (objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -') {
                            arrAuxiliar[10] = objResult[i].getValue(columns[13]);
                        } else
                            arrAuxiliar[10] = '';

                        //11. MUNICIPALIDAD
                        if (objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -') {
                            arrAuxiliar[11] = objResult[i].getValue(columns[14]);
                        } else
                            arrAuxiliar[11] = '';

                        // infoTxt = infoTxt +
                        //columna0 + columna1 + columna2 + columna3 + columna4 + columna5 + columna6 + columna7 + columna8 +
                        //columna9 +'\r\n';
                        ArrReteAux[_cont] = arrAuxiliar;
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
            return ArrReteAux;
        }


        function ObtieneRetencionReteFTE() {

            var _cont = 0;
            var intDMinReg = Number(paramBucle) * 1000;
            var intDMaxReg = intDMinReg + 1000;
            var DbolStop = false;
            var infoTxt = '';
            var arrAuxiliar = new Array();

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

            var periodFilterIni = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORAFTER,
                values: [paramperiodoInicio]
            });
            saved_search.filters.push(periodFilterIni);

            var periodFilterFin = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORBEFORE,
                values: [paramperiodoFinal]
            });
            saved_search.filters.push(periodFilterFin);

            var entityFilter = search.createFilter({
                name: 'name',
                operator: search.Operator.IS,
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
            //11.- Campo del LATAM - BASE AMOUNT LOCAL CURRENCY
            var montoBaseTaxResult = search.createColumn({
                name: 'formulacurrency',
                formula: '{custrecord_lmry_br_transaction.custrecord_lmry_base_amount_local_currc}',
                label: '11. BASE AMOUNT LOCAL CURRENCY'
            });
            saved_search.columns.push(montoBaseTaxResult);
            //12.-Campo del LATAM - AMOUNT LOCAL CURRENCY
            var montoLocalTaxResult = search.createColumn({
                name: 'formulacurrency',
                formula: '{custrecord_lmry_br_transaction.custrecord_lmry_amount_local_currency}',
                label: '12. AMOUNT LOCAL CURRENCY'
            });
            saved_search.columns.push(montoLocalTaxResult);
            //13.-Campo de description
            var descriptionTaxResult = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_br_transaction.custrecord_lmry_tax_description}',
                label: '13. Description'
            });
            saved_search.columns.push(descriptionTaxResult);

            //14.-Municipalidad
            var municipTransaction = search.createColumn({
                name: "internalid",
                join: "CUSTBODY_LMRY_MUNICIPALITY",
                label: "14. Municipality"
            });
            saved_search.columns.push(municipTransaction);

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

                        arrAuxiliar = new Array();
                        // 0. C?DIGO WHT
                        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
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

                        // exchange rate cabecera y multibook

                        if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != '') {
                            var ExchangerateAux = objResult[i].getValue(columns[10]);
                            ExchangerateC_S = exchange_rate(ExchangerateAux);
                        } else {
                            ExchangerateC_S = 1;
                        }

                        // 6. BASE IMPONIBLE
                        if (objResult[i].getValue(columns[11]) && objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 0) {
                            arrAuxiliar[6] = objResult[i].getValue(columns[11]);
                        } else {
                            if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                arrAuxiliar[6] = (parseFloat(objResult[i].getValue(columns[6]))) * (Number(ExchangerateC_S));

                            } else
                                arrAuxiliar[6] = '0.00';
                        }

                        // 7. RETENCION
                        if (objResult[i].getValue(columns[12]) && objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 0) {
                            arrAuxiliar[7] = objResult[i].getValue(columns[12]);
                        } else {
                            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {

                                arrAuxiliar[7] = (parseFloat(objResult[i].getValue(columns[7]))) * (Number(ExchangerateC_S));

                            } else
                                arrAuxiliar[7] = '0.00';
                        }

                        //8. FECHA
                        if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -') {
                            arrAuxiliar[8] = objResult[i].getValue(columns[8]);
                        } else
                            arrAuxiliar[8] = '';

                        //9. DESCRIPCION
                        if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -') {
                            arrAuxiliar[9] = Number(objResult[i].getValue(columns[9])).toFixed(2);
                        } else
                            arrAuxiliar[9] = '';

                        //10. DESCRIPCION NATIONAL TAX O CONTRIBUCION
                        if (objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -') {
                            arrAuxiliar[10] = objResult[i].getValue(columns[13]);
                        } else
                            arrAuxiliar[10] = '';

                        //11. MUNICIPALIDAD
                        if (objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -') {
                            arrAuxiliar[11] = objResult[i].getValue(columns[14]);
                        } else
                            arrAuxiliar[11] = '';

                        // infoTxt = infoTxt +
                        //columna0 + columna1 + columna2 + columna3 + columna4 + columna5 + columna6 + columna7 + columna8 +
                        //columna9 +'\r\n';
                        ArrReteAux[_cont] = arrAuxiliar;
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
            return ArrReteAux;
        }

        function ObtieneRetencionReteIVA() {

            var _cont = 0;
            var intDMinReg = Number(paramBucle) * 1000;
            var intDMaxReg = intDMinReg + 1000;
            var DbolStop = false;
            var infoTxt = '';
            var arrAuxiliar = new Array();

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

            var periodFilterIni = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORAFTER,
                values: [paramperiodoInicio]
            });
            saved_search.filters.push(periodFilterIni);

            var periodFilterFin = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORBEFORE,
                values: [paramperiodoFinal]
            });
            saved_search.filters.push(periodFilterFin);

            var entityFilter = search.createFilter({
                name: 'name',
                operator: search.Operator.IS,
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
            //11.- Campo del LATAM - BASE AMOUNT LOCAL CURRENCY
            var montoBaseTaxResult = search.createColumn({
                name: 'formulacurrency',
                formula: '{custrecord_lmry_br_transaction.custrecord_lmry_base_amount_local_currc}',
                label: '11. BASE AMOUNT LOCAL CURRENCY'
            });
            saved_search.columns.push(montoBaseTaxResult);
            //12.-Campo del LATAM - AMOUNT LOCAL CURRENCY
            var montoLocalTaxResult = search.createColumn({
                name: 'formulacurrency',
                formula: '{custrecord_lmry_br_transaction.custrecord_lmry_amount_local_currency}',
                label: '12. AMOUNT LOCAL CURRENCY'
            });
            saved_search.columns.push(montoLocalTaxResult);
            //13.-Campo de description
            var descriptionTaxResult = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_br_transaction.custrecord_lmry_tax_description}',
                label: '13. Description'
            });
            saved_search.columns.push(descriptionTaxResult);

            //14.-Municipalidad
            var municipTransaction = search.createColumn({
                name: "internalid",
                join: "CUSTBODY_LMRY_MUNICIPALITY",
                label: "14. Municipality"
            });
            saved_search.columns.push(municipTransaction);

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

                        arrAuxiliar = new Array();
                        // 0. C?DIGO WHT
                        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
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

                        // exchange rate cabecera y multibook

                        if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != '') {
                            var ExchangerateAux = objResult[i].getValue(columns[10]);
                            ExchangerateC_S = exchange_rate(ExchangerateAux);
                        } else {
                            ExchangerateC_S = 1;
                        }

                        // 6. BASE IMPONIBLE
                        if (objResult[i].getValue(columns[11]) && objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 0) {
                            arrAuxiliar[6] = objResult[i].getValue(columns[11]);
                        } else {
                            if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                arrAuxiliar[6] = (parseFloat(objResult[i].getValue(columns[6]))) * (Number(ExchangerateC_S));
                            } else
                                arrAuxiliar[6] = '0.00';
                        }

                        // 7. RETENCION
                        if (objResult[i].getValue(columns[12]) && objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 0) {
                            arrAuxiliar[7] = objResult[i].getValue(columns[12]);
                        } else {
                            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {
                                arrAuxiliar[7] = parseFloat(objResult[i].getValue(columns[7])) * (Number(ExchangerateC_S));
                            } else
                                arrAuxiliar[7] = '0.00';
                        }
                        
                        //8. FECHA
                        if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -') {
                            arrAuxiliar[8] = objResult[i].getValue(columns[8]);
                        } else
                            arrAuxiliar[8] = '';

                        //9. DESCRIPCION
                        if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -') {
                            arrAuxiliar[9] = Number(objResult[i].getValue(columns[9])).toFixed(2);
                        } else
                            arrAuxiliar[9] = '';

                        //10. DESCRIPCION NATIONAL TAX O CONTRIBUCION
                        if (objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -') {
                            arrAuxiliar[10] = objResult[i].getValue(columns[13]);
                        } else
                            arrAuxiliar[10] = '';

                        //11. MUNICIPALIDAD
                        if (objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -') {
                            arrAuxiliar[11] = objResult[i].getValue(columns[14]);
                        } else
                            arrAuxiliar[11] = '';
                        // infoTxt = infoTxt +
                        //columna0 + columna1 + columna2 + columna3 + columna4 + columna5 + columna6 + columna7 + columna8 +
                        //columna9 +'\r\n';
                        ArrReteAux[_cont] = arrAuxiliar;
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
            return ArrReteAux;
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
            

            var configpage = config.load({
                type: config.Type.COMPANY_INFORMATION
            });
            if (featSubsi) {
                companyname = ObtainNameSubsidiaria(paramsubsidi);
                companyruc = ObtainFederalIdSubsidiaria(paramsubsidi);
                companyaddress = ObtainAddressIdSubsidiaria(paramsubsidi);
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
                //libreria.sendMail(LMRY_script, ' [ ObtainNameSubsidiaria ] ' + err);
                libreriaGeneral.sendErrorEmail(err, LMRY_script, language);
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
                //libreria.sendMail(LMRY_script, ' [ ObtainFederalIdSubsidiaria ] ' + err);
                libreriaGeneral.sendErrorEmail(err, LMRY_script, language);
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
                //libreria.sendMail(LMRY_script, ' [ ObtainAddressIdSubsidiaria ] ' + err);
                libreriaGeneral.sendErrorEmail(err, LMRY_script, language);
            }
            return '';
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


        function ObtenerParametrosYFeatures() {
            //Parametros
            var objContext = runtime.getCurrentScript();

            paramsubsidi = objContext.getParameter({
                name: 'custscript_lmry_co_subsi_withbook_ret'
            });
            nameDIAN = objContext.getParameter({
                name: 'custscript_lmry_co_dian_name'
            });
            if (nameDIAN == null || nameDIAN == "- None -" || nameDIAN == "") {
                nameDIAN = ' ';
            }
            paramperiodoInicio = objContext.getParameter({
                name: 'custscript_lmry_co_periodini_withbook_re'
            });

            paramperiodoFinal = objContext.getParameter({
                name: 'custscript_lmry_co_periodfin_withbook_re'
            });

            paramMulti = objContext.getParameter({
                name: 'custscript_lmry_co_multibook_withbook_re'
            });

            paramidrpt = objContext.getParameter({
                name: 'custscript_lmry_co_idrpt_withbook_ret'
            });

            paramVendor = objContext.getParameter({
                name: 'custscript_lmry_co_vendor_withbook_ret'
            });

            paramTyreten = objContext.getParameter({
                name: 'custscript_lmry_co_type_withbook_ret'
            });

            paramCont = objContext.getParameter({
                name: 'custscript_lmry_co_cont_withbook_ret'
            });

            paramBucle = objContext.getParameter({
                name: 'custscript_lmry_co_bucle_withbook_ret'
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

            log.debug({
                title: 'ENTROfeats',
                details: paramidrpt + ' ' + paramMulti + ' ' + paramsubsidi + ' ' + paramperiodoFinal + ' ' + paramperiodoInicio + ' ' + paramVendor + ' ' + paramTyreten
            });

            log.debug({
                title: 'Puede ser 0 o vacio',
                details: nameDIAN + ' ' + paramCont + ' ' + paramBucle
            });

            var fecha_format = format.parse({
                value: paramperiodoInicio,
                type: format.Type.DATE
            });

            var MM = fecha_format.getMonth() + 1;
            var YYYY = fecha_format.getFullYear();
            var DD = fecha_format.getDate();

            if (('' + MM).length == 1) {
                MM = '0' + MM;
            }
            Inicio = Periodo(MM) + ' ' + YYYY;

            // Para buscar la municipality
            log.debug('paramVendor', paramVendor);
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
                log.debug({
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

        function getGlobalLabels() {
            var labels = {
                "titulo": {
                    "es": 'CERTIFICADO DE RETENCION',
                    "pt": 'CERTIFICADO DE RETENÇÃO',
                    "en": 'WITHHOLDING CERTIFICATE'
                },
                "articulo381": {
                    "es": 'Para dar Cumplimiento al articulo 381 de Estatuto Tributario, certificamos que durante el periodo ',
                    "pt": 'Para cumprimento do artigo 381.º do Regime Tributário, certificamos que durante o período ',
                    "en": 'To comply with article 381 of the Tax Statute, we certify that during the period '
                },
                "entre_inicio": {
                    "es": 'compredido entre el ',
                    "pt": 'entre ',
                    "en": 'between '
                },
                "entre_final": {
                    "es": ' y el ',
                    "pt": ' e ',
                    "en": ' and '
                },
                "practicaIca": {
                    "es": ' , practicamos retenciones a titulo de ICA.',
                    "pt": ' , praticamos retenções por meio do ICA.',
                    "en": ' , We practice withholdings by way of ICA.'
                },
                "practicaRenta": {
                    "es": ' , practicamos retenciones a titulo de RENTA.',
                    "pt": ' , praticamos retenções por meio do RENTA.',
                    "en": ' , We practice withholdings by way of RENTA.'
                },
                "practicaIva": {
                    "es": ' , practicamos retenciones a titulo de IVA.',
                    "pt": ' , praticamos retenções por meio do IVA.',
                    "en": ' , We practice withholdings by way of IVA.'
                },
                "concepto": {
                    "es": 'CONCEPTO',
                    "pt": 'CONCEITO',
                    "en": 'CONCEPT'
                },
                "nfactura": {
                    "es": 'N. FACTURA',
                    "pt": 'N. FATURA',
                    "en": 'N. INVOICE'
                },
                "retencion": {
                    "es": 'RETENCION',
                    "pt": 'RETENÇÃO',
                    "en": 'BASE'
                },
                "base": {
                    "es": 'BASE',
                    "pt": 'BASE DE',
                    "en": 'RETENTION'
                },
                "porc": {
                    "es": 'PORC.',
                    "pt": 'PERC.',
                    "en": 'PERC.'
                },
                "valor": {
                    "es": 'VALOR.',
                    "pt": 'VALOR',
                    "en": 'RETAINED'
                },
                "retenido": {
                    "es": 'RETENIDO',
                    "pt": 'RETIDO',
                    "en": 'VALUE'
                },
                "pieReteIca1": {
                    "es": 'Los valores retenidos fueron consignados en la Ciudad de ',
                    "pt": 'Os valores retidos foram tempestivamente consignados na cidade de ',
                    "en": 'The amounts withheld were timely consigned in the city of '
                },
                "pieReteIca2": {
                    "es": ' en la Ciudad de ',
                    "en": ' in the city of ',
                    "pt": ' na cidade de '
                },
                "pieReteOtros1": {
                    "es": 'Los valores retenidos fueron consignados oportunamente a favor de la DIRECCION DE IMPUESTOS Y ADUANAS NACIONALES DIAN en la Ciudad de ',
                    "pt": 'Os valores retidos foram oportunamente consignados em favor da DIRETORIA NACIONAL TRIBUTÁRIA E ADUANEIRA DIAN na Cidade de ',
                    "en": 'The amounts withheld were timely consigned in favor of the NATIONAL TAX AND CUSTOMS DIRECTORATE DIAN in the City of '
                },
                "firma": {
                    "es": 'SE EXPIDE SIN FIRMA AUTOGRAFA',
                    "pt": 'EMITIDO SEM ASSINATURA DE AUTÓGRAFO',
                    "en": 'ISSUED WITHOUT AUTOGRAPH SIGNATURE'
                },
                "domicilio": {
                    "es": 'DOMICILIO PRINCIPAL: ',
                    "pt": 'RESIDÊNCIA PRIMÁRIA: ',
                    "en": 'PRIMARY RESIDENCE: '
                },
                "fechaExpedicion": {
                    "es": 'FECHA DE EXPEDICION: ',
                    "pt": 'DATA DE EXPEDIÇÃO: ',
                    "en": 'EXPEDITION DATE: '
                },
                "nodata": {
                    "es": 'No existe informacion para los criterios seleccionados.',
                    "pt": 'Não há informações para os critérios selecionados.',
                    "en": 'There is no information for the selected criteria.'
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