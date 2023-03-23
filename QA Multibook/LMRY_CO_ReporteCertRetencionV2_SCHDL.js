/**
 * Module Description
 *
 * Version    Date            Author           Remarks
 * 1.00       16 Dec 2014     LatamReady Consultor
 * File: LMRY_CO_ReporteCertRetencionV2_SCHDL.js
 */
var objContext = nlapiGetContext();
// Nombre del Reporte
var namereport = "Reporte de Certificado de Retencion";
var LMRY_script = 'LMRY CO Reportes Certificado de Retencion SCHDL';
//Parametros
var paramsubsidi = '';
var paramperiodinicio = '';
var paramperiodfinal = '';
var paramperiodinicio_aux = '';
var paramperiodfinal_aux = '';

var paramvendor = '';
var paramtreten = '';
var paramidrpt = '';
var paramMultibook = '';
//Control de Reporte
var periodstartdate = '';
var periodenddate = '';
var antperiodenddate = '';
var configpage = '';
var companyruc = '';
var companyname = '';
var companyaddress = '';
var companyname_vendor = '';
var nit_vendor = '';

var fechalog = '';
var ArrReteAux = new Array();
var ArrRetencion = new Array();

var strName = '';
var periodname = '';
var auxmess = '';
var auxanio = '';
var strConcepto = '';

var columnas_f = new Array();

var multibook = '';
/******************************************
 * @leny - Modificado el 28/08/2015
 * Nota: Variables para acumulacions de Montos.
 ******************************************/
var montototal = 0;
var montoBase = 0;
var num_muni = 0;
var nameDIAN = '';
var municipality_id = 0;
var municipality = 'Bogota';
var jsonTransactionMunicip = {};
/* ***********************************************
 * Arreglo con la structura de la tabla log
 * ******************************************** */
var RecordName = 'customrecord_lmry_co_rpt_generator_log';
var RecordTable = ['custrecord_lmry_co_rg_name',
    'custrecord_lmry_co_rg_postingperiod',
    'custrecord_lmry_co_rg_subsidiary',
    'custrecord_lmry_co_rg_url_file',
    'custrecord_lmry_co_rg_employee',
    'custrecord_lmry_co_rg_multibook',
    'custrecord_lmry_co_rg_transaction'
];

var featuresubs = objContext.getFeature('SUBSIDIARIES');
var feamultibook = objContext.getFeature('MULTIBOOK');
var featuremultib = objContext.getFeature('MULTIBOOKMULTICURR');

var result_f;

/* ***********************************************
 * Inicia el proceso Schedule
 * ******************************************** */
function scheduled_main_LMRYCertRetencion(type) {

    //record
    columnas_f[0] = new nlobjSearchColumn('name');
    columnas_f[1] = new nlobjSearchColumn('custrecord_lmry_wht_codedesc');
    columnas_f[2] = new nlobjSearchColumn('custrecord_lmry_wht_coderate');

    result_f = nlapiSearchRecord('customrecord_lmry_wht_code', null, null, columnas_f);



    nameDIAN = objContext.getSetting('SCRIPT', 'custscript_lmry_co_dian_name');
    if (nameDIAN == null || nameDIAN == "- None -" || nameDIAN == "") {
        nameDIAN = " ";
    }
    // parametro-
    paramperiodinicio = objContext.getSetting('SCRIPT', 'custscript_lmry_co_periodini_withbook');
    paramperiodfinal = objContext.getSetting('SCRIPT', 'custscript_lmry_co_periodfin_withbook');
    paramsubsidi = objContext.getSetting('SCRIPT', 'custscript_lmry_co_subsi_withbook');
    paramvendor = objContext.getSetting('SCRIPT', 'custscript_lmry_co_vendor_withbook');
    paramtreten = objContext.getSetting('SCRIPT', 'custscript_lmry_co_type_withbook');
    paramidrpt = objContext.getSetting('SCRIPT', 'custscript_lmry_co_idrpt_withbook');
    paramMultibook = objContext.getSetting('SCRIPT', 'custscript_lmry_co_multibook_withbook');


    var aux_param_inicio = nlapiStringToDate(paramperiodinicio);
    var MM_ini = aux_param_inicio.getMonth() + 1;
    var YYYY_ini = aux_param_inicio.getFullYear();
    var DD_ini = aux_param_inicio.getDate();

    if (('' + MM_ini).length == 1) {
        MM_ini = '0' + MM_ini;
    }
    paramperiodinicio_aux = DD_ini + '/' + MM_ini + '/' + YYYY_ini;

    var aux_param_fin = nlapiStringToDate(paramperiodfinal);
    var MM_fin = aux_param_fin.getMonth() + 1;
    var YYYY_fin = aux_param_fin.getFullYear();
    var DD_fin = aux_param_fin.getDate();

    if (('' + MM_fin).length == 1) {
        MM_fin = '0' + MM_fin;
    }
    paramperiodfinal_aux = DD_fin + '/' + MM_fin + '/' + YYYY_fin;

    nlapiLogExecution('DEBUG', 'parametro-> ', paramMultibook + ',' + paramperiodinicio_aux + ',' + paramperiodfinal_aux);
    nlapiLogExecution('DEBUG', 'parametro-> ', paramperiodinicio + ',' + paramperiodfinal + ',' + paramsubsidi + ',' + paramvendor + ',' + paramtreten + ',' + paramidrpt);

    ObtainVendor(paramvendor);

    if (paramtreten == 1) {

        //* Para buscar la municipality x Subsidiaria
        if (paramsubsidi != '' && paramsubsidi != null) {
            var municipality_id_subsi = nlapiLookupField('subsidiary', paramsubsidi, 'custrecord_lmry_municipality_sub');
        } else {
            configpage = nlapiLoadConfiguration('companyinformation');
            var municipality_id_subsi = configpage.getFieldValue('custrecord_lmry_municipality_sub') || '';
        }
        if (municipality_id_subsi != '') {
            var muniBySubsidiaria = genNameMunicipality(municipality_id_subsi);
        } else {
            var muniBySubsidiaria = '';
        }
        nlapiLogExecution('DEBUG', 'muniBySubsidiaria: ' + municipality_id_subsi, muniBySubsidiaria);

        //* Para buscar la municipality x VENDOR
        if (paramvendor != '' && paramvendor != null) {
            var municipality_id = nlapiLookupField('vendor', paramvendor, ['custentity_lmry_municipality']);
            var municipality_id_vendor = municipality_id.custentity_lmry_municipality;
        }
        if (municipality_id_vendor != '') {
            var nameMuniByVendor = genNameMunicipality(municipality_id_vendor);
            nlapiLogExecution('DEBUG', 'nameMuniByVendor: ' + municipality_id_vendor, nameMuniByVendor);
        }

        if (feamultibook) {
            multibook = nlapiLookupField('accountingbook', paramMultibook, 'name');
        }

        if (nameMuniByVendor != '' || muniBySubsidiaria != '') {
            municipality = nameMuniByVendor || muniBySubsidiaria;
        }
    } else {

        var vendorSearch = nlapiSearchRecord("vendor", null, [
            ["internalid", "anyof", paramvendor]
        ], [
            new nlobjSearchColumn("custrecord_lmry_addr_city", "Address", null)
        ]);
        if (vendorSearch != null && vendorSearch.length != 0) {
            var nameMuni = vendorSearch[0].getText("custrecord_lmry_addr_city", "Address", null);
        }
        if (nameMuni != '' && nameMuni != null) {
            nameMuni = nameMuni.replace('BOGOTA BOGOTA, D.C.', 'BOGOTA');
        }
        municipality = nameMuni || 'BOGOTA';
    }

    //ReteICA
    if (paramtreten == 1) {
        strNameFile = 'COCertificadoReteICA';
        ArrRetencion = ObtieneRetencion('customsearch_lmry_co_reteica_compras');
        strConcepto = 'Retencion ICA';
    }
    // ReteFTE
    if (paramtreten == 2) {
        strNameFile = 'COCertificadoReteFte';
        strConcepto = 'Retencion en la Fuente';
        ArrRetencion = ObtieneRetencion('customsearch_lmry_co_retefte_compras');
    }
    // ReteIVA
    if (paramtreten == 3) {
        strNameFile = 'COCertificadoReteIVA';
        strConcepto = 'Retencion IVA';
        ArrRetencion = ObtieneRetencion('customsearch_lmry_co_reteiva_compras');
    }


    if (ArrRetencion.length != null && ArrRetencion.length != 0) {
        nlapiLogExecution('DEBUG', 'Tam ArrRetencion', ArrRetencion.length);
        if (paramtreten == 1) {
            jsonTransactionMunicip = obtenerTransaccionesXMunicipalidad(ArrRetencion);

            for (key in jsonTransactionMunicip) {
                municipality = key;
                name_muni = key.split(' ').join('_');
                ArrRetencion = jsonTransactionMunicip[key];
                nlapiLogExecution('DEBUG', 'Muni: tam[' + ArrRetencion.length + '] - key ' + key, ArrRetencion);
                nlapiLogExecution('DEBUG', 'name_muni', name_muni);
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

    //ObtienePeriodoContable();
    //CapturaInfoPeriodo(paramperiodo);

}

function obtenerTransaccionesXMunicipalidad(ArrRetencion) {
    var jsonAgrupadoxMun = {};
    for (var i = 0; i < ArrRetencion.length; i++) {

        if (ArrRetencion[i][6] != '') {
            ArrRetencion[i][6] = genNameMunicipality(ArrRetencion[i][6]);
        }

        var municipalidad = ArrRetencion[i][6] || municipality;

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
        montoBase = parseFloat(montoBase) + parseFloat(ArrRetencion[i][3]);
        montototal = parseFloat(montototal) + parseFloat(ArrRetencion[i][4]);

        strAux += "<tr>";
        strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\">";
        strAux += "<p>" + ArrRetencion[i][0].replace(/&/g, 'and') + "</p>";
        strAux += "</td>";
        strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\">";
        strAux += "<p>" + ArrRetencion[i][2].replace(/&/g, 'and') + "</p>";
        strAux += "</td>";
        strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
        strAux += "<p>" + FormatoNumero(parseFloat(ArrRetencion[i][3]).toFixed(2), "$") + "</p>";
        strAux += "</td>";
        strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
        strAux += "<p>" + ArrRetencion[i][5] + "%</p>";
        strAux += "</td>";
        strAux += "<td style=\"text-align: center; font-size: 9pt; border: 1px solid #000000\" align=\"right\">";
        strAux += "<p>" + FormatoNumero(parseFloat(ArrRetencion[i][4]).toFixed(2), "$") + "</p>";
        strAux += "</td>";
        strAux += "</tr>";

    }

    return strAux;
}

function RecordNoData() {
    //log.error('ENTRO', 'entro RecordNoData');
    var usuario = objContext.getName();
    var subsidi = objContext.getSetting('SCRIPT', 'custscript_lmry_co_subsi_withbook');
    nlapiLogExecution('DEBUG', 'NO DATA subsidiaria', subsidi);
    if (subsidi != null && subsidi != '') {
        subsidi = nlapiLookupField('subsidiary', subsidi, 'legalname');
        nlapiLogExecution('DEBUG', 'NO DATA subsidiaria', subsidi);
    } else {
        configpage = nlapiLoadConfiguration('companyinformation');
        subsidi = configpage.getFieldValue('legalname');
    }

    var record = nlapiLoadRecord('customrecord_lmry_co_rpt_generator_log', paramidrpt); // generator_log
    record.setFieldValue('custrecord_lmry_co_rg_name', 'No existe informacion para los criterios seleccionados'); // name
    record.setFieldValue('custrecord_lmry_co_rg_postingperiod', fechalog); // postingperiod
    record.setFieldValue('custrecord_lmry_co_rg_subsidiary', subsidi); // subsidiary

    record.setFieldValue('custrecord_lmry_co_rg_employee', usuario); // employee
    if (multibook != '')
        record.setFieldValue('custrecord_lmry_co_rg_multibook', multibook);

    nlapiSubmitRecord(record, true);

}
//-------------------------------------------------------------------------------------------------------
//Generaci?n archivo PDF
//-------------------------------------------------------------------------------------------------------
function GeneracionPDF() {
    // Declaracion de variables
    var strName = '';
    montototal = 0;

    // Datos del Periodo
    var Auxiliar = paramperiodinicio.split('/');
    //paramperiodinicio = '01/'+RellenaTexto(Auxiliar[1],2,'N')+'/'+Auxiliar[2];

    var Auxiliar = paramperiodfinal.split('/');
    //paramperiodfinal =  RellenaTexto(Auxiliar[0],2,'N')+'/'+RellenaTexto(Auxiliar[1],2,'N')+'/'+Auxiliar[2];

    //nlapiLogExecution('DEBUG', 'parametro-> ',paramperiodinicio+','+paramperiodfinal);
    var inicio_arr = paramperiodinicio_aux.split('/');
    var fin_arr = paramperiodfinal_aux.split('/');

    var sPeriodoIni = TraePeriodo(inicio_arr[1]) + ' ' + inicio_arr[2];
    var sPeriodoFina = TraePeriodo(fin_arr[1]) + ' ' + fin_arr[2];
    fechalog = sPeriodoIni;

    // Datos de la empresa

    var featuresubs = nlapiGetContext().getFeature('SUBSIDIARIES');

    if (featuresubs == true) {
        ObtainSubsidiaria(paramsubsidi);
    } else {
        configpage = nlapiLoadConfiguration('companyinformation');
        companyruc = configpage.getFieldValue('employerid');
        companyname = configpage.getFieldValue('legalname');
        companyaddress = configpage.getFieldValue('mainaddress_text');
    }

    companyname = ValidarAcentos(companyname);
    //-------------------------------------------------------------------------------------------------------
    //Cabecera del reporte
    //-------------------------------------------------------------------------------------------------------
    var strHead = '';
    strHead += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
    strHead += "<tr>";
    strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\" align=\"center\">";
    strHead += "<p>" + companyname + "</p>";
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
    strHead += "<p>CERTIFICADO DE RETENCION</p>";
    strHead += "</td>";
    strHead += "</tr>";
    // Impuesto ICA
    if (paramtreten == 1) {
        strHead += "<tr>";
        strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
        strHead += "Para dar Cumplimiento al articulo 381 de Estatuto tributario, certificamos que durante el periodo ";
        strHead += "comprendido entre el " + paramperiodinicio_aux + " y el " + paramperiodfinal_aux + " , practicamos retenciones a titulo de ICA.";
        strHead += "</td>";
        strHead += "</tr>";
    }
    // Impuesto RENTA
    if (paramtreten == 2) {
        strHead += "<tr>";
        strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
        strHead += "Para dar cumplimiento al articulo 381 de Estatuto Tributario, certificamos que durante el periodo ";
        strHead += "comprendido entre el " + paramperiodinicio_aux + " y el " + paramperiodfinal_aux + " , practicamos retenciones a titulo de RENTA.";
        strHead += "</td>";
        strHead += "</tr>";
    }
    // Impuesto IVA
    if (paramtreten == 3) {
        strHead += "<tr>";
        strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
        strHead += "Para dar cumplimiento al articulo 381 de Estatuto Tributario, certificamos que durante el periodo ";
        strHead += "comprendido entre el " + paramperiodinicio_aux + " y el " + paramperiodfinal_aux + " , practicamos retenciones a titulo de IVA.";
        strHead += "</td>";
        strHead += "</tr>";
    }

    strHead += "</table>";

    strHead += "<p></p>";
    strHead += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
    strHead += "<tr>";
    strHead += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
    strHead += "<p>" + companyname_vendor.replace(/&/g, 'and') + "</p>";
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
    strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
    strDeta += "<p>CONCEPTO</p>";
    strDeta += "</td>";
    strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"30mm\">";
    strDeta += "<p>N. FACTURA</p>";
    strDeta += "</td>";
    strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
    strDeta += "BASE<br/>";
    strDeta += "RETENCION";
    strDeta += "</td>";
    strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"50mm\">";
    strDeta += "<p>PORC.</p>";
    strDeta += "</td>";
    strDeta += "<td style=\"text-align: center; font-weight: bold; font-size: 9pt; border: 1px solid #000000\" align=\"center\" width=\"40mm\">";
    strDeta += "VALOR<br/>";
    strDeta += "RETENIDO";
    strDeta += "</td>";
    strDeta += "</tr>";

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


    // cierra la tabla
    strDeta += "</table>";

    strName += strDeta;


    //-------------------------------------------------------------------------------------------------------
    //Pie de p?gina del reporte
    //-------------------------------------------------------------------------------------------------------
    var strNpie = '';
    strNpie += "<p></p>";
    if (paramtreten == 1) //RETEICA
    {
        strNpie += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
        strNpie += "<tr>";
        strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
        strNpie += "Los valores retenidos fueron consignados en la Ciudad de " + municipality + ".";
        strNpie += "</td>";
        strNpie += "</tr>";
        strNpie += "</table>";
    } else {
        strNpie += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
        strNpie += "<tr>";
        strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
        strNpie += "Los valores retenidos fueron consignados oportunamente a favor de la DIRECCION DE IMPUESTOS Y ADUANAS NACIONALES DIAN en la Ciudad de " + municipality + ".";
        strNpie += "</td>";
        strNpie += "</tr>";
        strNpie += "</table>";
    }
    strNpie += "<p></p>";
    strNpie += "<table style=\"font-family: Verdana, Arial, Helvetica, sans-serif; width:100%\">";
    strNpie += "<tr>";
    strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
    strNpie += "SE EXPIDE SIN FIRMA AUTOGRAFA";
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
        if (auxDireccion == 'undefined' || auxDireccion == '' || auxDireccion == null || auxDireccion == '- None -') {
            auxDireccion = '';
        }
        companyaddress = ValidarAcentos(companyaddress);
        auxDireccion = ValidarAcentos(auxDireccion);

        //nlapiLogExecution('DEBUG', 'auxDireccion-> ', auxDireccion);
        //nlapiLogExecution('DEBUG', 'auxDireccion1-> ', companyaddress.split('\n')[1]);
        companyaddress = 'xxxx';
    }

    var fecha_actual = new Date();
    fecha_actual = fecha_actual.getDate() + "/" + (fecha_actual.getMonth() + 1) + "/" + fecha_actual.getFullYear();

    strNpie += "<tr>";
    strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
    strNpie += "DOMICILIO PRINCIPAL: " + auxDireccion.replace(/&/g, 'and');
    strNpie += "</td>";
    strNpie += "</tr>";
    strNpie += "<tr>";
    strNpie += "<td style=\"text-align: center; font-size: 10pt; border: 0px solid #000000\">";
    strNpie += "FECHA DE EXPEDICION: " + fecha_actual;
    strNpie += "</td>";
    strNpie += "</tr>";
    strNpie += "</table>";
    strNpie += "<p></p>";

    strName += strNpie;
    //nlapiLogExecution('DEBUG', 'strName-> ', strName);

    var xml = "<?xml version=\"1.0\"?>\n<!DOCTYPE pdf PUBLIC \"-//big.faceless.org//report\" \"report-1.1.dtd\">\n";
    xml += '<pdf><head><style> body {size:A4}</style></head><body>';
    xml += strName;
    xml += "</body>\n</pdf>";
    //nlapiLogExecution('DEBUG', 'xml-> ', xml);

    //	nlapiLogExecution('DEBUG', 'xml-> ',xml);
    var strfile = nlapiXMLToPDF(xml);

    if (featuremultib) {
        var _NameFile = strNameFile + "_" + companyname + "_" + companyname_vendor + "_" + sPeriodoIni + "_" + sPeriodoFina + "_" + name_muni + "_" + paramMultibook + ".pdf";
    } else {
        var _NameFile = strNameFile + "_" + companyname + "_" + companyname_vendor + "_" + sPeriodoIni + "_" + sPeriodoFina + "_" + name_muni + ".pdf";
    }
    savefile(_NameFile, strfile);
}

//-------------------------------------------------------------------------------------------------------
//Obtiene Retenciones ICA-Compras: Latam - CO ReteICA en las Compras
//Latam - CO ReteFuente en las Compras / Latam - CO ReteIVA en las Compras
//-------------------------------------------------------------------------------------------------------
function ObtieneRetencion(pBusqueda) {
    //	try
    //	{
    // Control de Memoria
    var intDMaxReg = 1000;
    var intDMinReg = 0;
    var arrAuxiliar = new Array();
    var aux = new Array();
    // Exedio las unidades
    var Dusager = false;
    var DbolStop = false;
    var usageRemaining = objContext.getRemainingUsage();

    // Valida si es OneWorld
    var featuresubs = nlapiGetContext().getFeature('SUBSIDIARIES');
    var _cont = 0;

    var objCont = nlapiGetContext();
    var featuremultib = objCont.getFeature('MULTIBOOKMULTICURR');

    var savedsearch = nlapiLoadSearch('transaction', pBusqueda);
    // Valida si es OneWorld
    if (featuresubs == true) {
        savedsearch.addFilter(new nlobjSearchFilter('subsidiary', null, 'is', paramsubsidi));
    }
    if (paramperiodinicio != null && paramperiodinicio != '') {
        savedsearch.addFilter(new nlobjSearchFilter('trandate', null, 'onorafter', paramperiodinicio));
    }
    if (paramperiodfinal != null && paramperiodfinal != '') {
        savedsearch.addFilter(new nlobjSearchFilter('trandate', null, 'onorbefore', paramperiodfinal));
    }
    savedsearch.addFilter(new nlobjSearchFilter('formulatext', null, 'is', paramvendor).setFormula("{vendor.internalid}"));

    savedsearch.addColumn(new nlobjSearchColumn("formulatext", null, "GROUP").setFormula("{type.id}")); // pos 8
    savedsearch.addColumn(new nlobjSearchColumn("internalid", "CUSTBODY_LMRY_MUNICIPALITY", "GROUP")); // pos 9

    if (featuremultib == true || featuremultib == 'T') {
        savedsearch.addFilter(new nlobjSearchFilter('accountingbook', 'accountingtransaction', 'anyof', paramMultibook));
        savedsearch.addColumn(new nlobjSearchColumn("formulacurrency", null, "SUM").setFormula("{accountingtransaction.amount}")); // pos 10
    }


    var searchresult = savedsearch.runSearch();


    while (!DbolStop && objContext.getRemainingUsage() > 200) {
        var objResult = searchresult.getResults(intDMinReg, intDMaxReg);
        var intLength = objResult.length;

        if (objResult != null && intLength > 0) {

            for (var i = 0; i < intLength; i++) {
                var columns = objResult[i].getAllColumns();

                if (i == intLength) {
                    break;
                }
                if (usageRemaining <= 200 && (i + 1) < objResult.length) {
                    var status = nlapiScheduleScript(objContext.getScriptId(), objContext.getDeploymentId(), _params);
                    if (status == 'QUEUED') {
                        break;
                    }
                }

                arrAuxiliar = new Array();

                var tasa = calcular_tasa(objResult[i].getValue(columns[1]));

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
                //2. NuMERO DE TRANSACCI?N
                if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'undefined') {
                    arrAuxiliar[2] = objResult[i].getValue(columns[4]);
                } else {
                    arrAuxiliar[2] = '';
                }
                //3. Base Imponible
                if (featuremultib == true || featuremultib == 'T') {
                    arrAuxiliar[3] = Math.abs(Number(objResult[i].getValue(columns[10])));
                } else {
                    arrAuxiliar[3] = Math.abs(Number(objResult[i].getValue(columns[5])));
                }
                if (objResult[i].getValue(columns[8]) == 'VendCred') {
                    arrAuxiliar[3] = arrAuxiliar[3] * -1;
                }
                //4.RETENCIoN
                if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'undefined') {
                    arrAuxiliar[4] = Math.abs(Number(objResult[i].getValue(columns[6])));
                    //arrAuxiliar[4] = objResult[i].getValue(columns[6]);
                    if (objResult[i].getValue(columns[8]) == 'VendCred') {
                        arrAuxiliar[4] = arrAuxiliar[4] * -1;
                    }
                } else {
                    arrAuxiliar[4] = '0.00';
                }
                //5.RATE
                if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'undefined') {
                    //arrAuxiliar[5] = Number(objResult[i].getValue(columns[7]));
                    arrAuxiliar[5] = Number(objResult[i].getValue(columns[2])).toFixed(2);
                } else {
                    arrAuxiliar[5] = '';
                }

                //6.MUNICIPALIDAD
                if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'undefined') {
                    arrAuxiliar[6] = objResult[i].getValue(columns[9]);
                } else {
                    arrAuxiliar[6] = '';
                }

                ArrReteAux[_cont] = arrAuxiliar;
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
    /*}
    catch(error) {
    	sendemail(' [ ObtieneReteICA ] ' +error, LMRY_script);
    }*/
    return ArrReteAux;
}

function genNameMunicipality(id_municipality) {
    if (id_municipality != '' && id_municipality != null) {
        var code_mun = nlapiLookupField('customrecord_lmry_co_entitymunicipality', id_municipality, 'custrecord_lmry_co_municcode');
        nlapiLogExecution('DEBUG', 'code_municipality', code_mun);

        var citySearch = nlapiSearchRecord("customrecord_lmry_city", null, [
            ["custrecord_lmry_city_country", "anyof", "48"],
            "AND", ["custrecord_lmry_city_id", "is", code_mun]
        ], [
            new nlobjSearchColumn("name").setSort(false)
        ]);
        if (citySearch != null && citySearch.length != 0) {
            var nameMuni = citySearch[0].getValue("name");
        }
        if (nameMuni != '' && nameMuni != null) {
            nameMuni = nameMuni.replace('BOGOTA BOGOTA, D.C.', 'BOGOTA');
        }
        return nameMuni;
    } else {
        return '';
    }
}

function ObtienePeriodoContable(pFecha) {
    // Seteo de Porcentaje completo
    objContext.setPercentComplete(0.00);

    // Control de Memoria
    var intDMaxReg = 1000;
    var intDMinReg = 0;
    var PeriodName = '';

    // Exedio las unidades
    var Dusager = false;
    var DbolStop = false;
    var usageRemaining = objContext.getRemainingUsage();

    // Valida si es OneWorld
    var featuresubs = nlapiGetContext().getFeature('SUBSIDIARIES');

    // Consulta de Cuentas
    var savedsearch = nlapiLoadSearch('accountingperiod', 'customsearch_lmry_idaccountingperiod');
    if (paramperiodinicio != null && paramperiodinicio != '') {
        savedsearch.addFilter(new nlobjSearchFilter('startdate', null, 'on', pFecha)); //enddate
    }

    var searchresult = savedsearch.runSearch();
    while (!DbolStop && objContext.getRemainingUsage() > 200) {
        var objResult = searchresult.getResults(intDMinReg, intDMaxReg);

        if (objResult != null) {
            var intLength = objResult.length;

            for (var i = 0; i < intLength; i++) {
                if (usageRemaining <= 200 && (i + 1) < objResult.length) {
                    var status = nlapiScheduleScript(objContext.getScriptId(), objContext.getDeploymentId(), _params);
                    if (status == 'QUEUED') {
                        break;
                    }
                }
                columns = objResult[i].getAllColumns();
                nlapiLogExecution('DEBUG', 'name fecha-> ', '' + objResult[i].getValue(columns[1]));
                //0. name
                if (objResult[i].getValue(columns[1]) != null)
                    PeriodName = objResult[i].getValue(columns[1]);
                else
                    PeriodName = '';

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
    return PeriodName;
    //nlapiLogExecution('DEBUG', 'ArrPeriodos.length-> ',ArrPeriodos.length);

}
//-------------------------------------------------------------------------------------------------------
//Graba el archivo en el Gabinete de Archivos
//-------------------------------------------------------------------------------------------------------
function savefile(pNombreFile, pStrName) {
    // Ruta de la carpeta contenedora
    var FolderId = nlapiGetContext().getSetting('SCRIPT', 'custscript_lmry_file_cabinet_rg_co');


    // Almacena en la carpeta de Archivos Generados
    if (FolderId != '' && FolderId != null) {
        // Genera el nombre del archivo
        var NameFile = pNombreFile;

        // Crea el archivo
        var File = nlapiCreateFile(NameFile, 'PDF', pStrName.getValue());
        File.setFolder(FolderId);

        // Termina de grabar el archivo
        var idfile = nlapiSubmitFile(File);

        // Trae URL de archivo generado
        var idfile2 = nlapiLoadFile(idfile);

        // Obtenemo de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
        var getURL = objContext.getSetting('SCRIPT', 'custscript_lmry_netsuite_location');
        var urlfile = '';
        if (getURL != '' && getURL != '') {
            urlfile += 'https://' + getURL;
        }
        urlfile += idfile2.getURL();

        //Genera registro personalizado como log
        if (idfile) {
            var usuario = objContext.getName();
            var reportName = 'CO - Certificado de Retención';
            var subsidi = objContext.getSetting('SCRIPT', 'custscript_lmry_co_subsi_withbook');
            if (subsidi != null && subsidi != '') {
                subsidi = nlapiLookupField('subsidiary', subsidi, 'legalname');
            } else {
                configpage = nlapiLoadConfiguration('companyinformation');
                subsidi = configpage.getFieldValue('legalname');
            }
            var tmdate = new Date();
            var myDate = nlapiDateToString(tmdate);
            var myTime = nlapiDateToString(tmdate, 'timeofday');
            //  var current_date = myDate + ' ' + myTime;

            // Se graba el en log de archivos generados del reporteador
            if (num_muni > 0) {
                var record = nlapiCreateRecord(RecordName); // generator_log
            } else {
                var record = nlapiLoadRecord(RecordName, paramidrpt); // generator_log
            }
            record.setFieldValue(RecordTable[0], NameFile); // name
            record.setFieldValue(RecordTable[1], fechalog); // postingperiod
            record.setFieldValue(RecordTable[2], subsidi); // subsidiary
            record.setFieldValue(RecordTable[3], urlfile); // url_file
            record.setFieldValue(RecordTable[4], usuario); // employee

            num_muni++;

            if (multibook != '')
                record.setFieldValue(RecordTable[5], multibook);

            record.setFieldValue(RecordTable[6], reportName); // Report Name
            nlapiSubmitRecord(record, true);

            sendrptuser(NameFile);
        }
    } else {
        // Debug
        nlapiLogExecution('DEBUG', 'Creacion de PDF', 'No se existe el folder');
    }
}



//-------------------------------------------------------------------------------------------------------
//Obtiene Informacion Vendor: CompanyName / VatRegNumber
//-------------------------------------------------------------------------------------------------------
function ObtainVendor(idvendor) {
    try {
        if (idvendor != '' && idvendor != null) {
            var columnFrom = nlapiLookupField('vendor', idvendor, ['companyname', 'vatregnumber', 'custentity_lmry_digito_verificator', 'isperson', 'firstname', 'lastname']);

            if (columnFrom.isperson != "F") {
                var denom = columnFrom.firstname + " " + columnFrom.lastname;
            } else {
                var denom = columnFrom.companyname;
            }
            companyname_vendor = ValidarAcentos(denom);

            if (columnFrom.vatregnumber != null && columnFrom.vatregnumber != "" && columnFrom.vatregnumber != "- None -") {
                var vatregnumber = columnFrom.vatregnumber;
                if (columnFrom.custentity_lmry_digito_verificator != null && columnFrom.custentity_lmry_digito_verificator != "" && columnFrom.custentity_lmry_digito_verificator != "- None -") {
                    nit_vendor = columnFrom.vatregnumber + "-" + columnFrom.custentity_lmry_digito_verificator.substr(0, 1);
                } else {
                    nit_vendor = columnFrom.vatregnumber + "-" + " ";
                }
            } else {
                nit_vendor = "             ";
            }
        }
    } catch (err) {
        sendemail(' [ ObtainVendor ] ' + err, LMRY_script);
    }
    return true;
}

function ValidarAcentos(s) {
    var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ&°–—ªº·&";
    var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyyo--ao. ";

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
//Obtiene Subsidiaria
//-------------------------------------------------------------------------------------------------------
function ObtainSubsidiaria(subsidiari) {
    //try{
    // Seteo de Porcentaje completo
    objContext.setPercentComplete(0.00);

    // Control de Memoria
    var intDMaxReg = 1000;
    var intDMinReg = 0;

    // Exedio las unidades
    var Dusager = false;
    var DbolStop = false;
    var usageRemaining = objContext.getRemainingUsage();

    // Filtros de la transaccion
    var filters = new Array();
    filters[0] = new nlobjSearchFilter('isinactive', null, 'is', 'F');
    filters[1] = new nlobjSearchFilter('internalid', null, 'is', subsidiari);
    // Columnas de la Transaccion
    var columns = new Array();
    //columns[0] = new nlobjSearchColumn('internalid');
    columns[0] = new nlobjSearchColumn('legalname');
    columns[1] = new nlobjSearchColumn('taxidnum');
    columns[2] = new nlobjSearchColumn('address', 'address');


    // Consulta de Cuentas
    var objResult = nlapiSearchRecord('subsidiary', null, filters, columns);
    if (objResult != null) {
        var intLength = objResult.length;
        for (var i = 0; i < intLength; i++) {
            columns = objResult[i].getAllColumns();

            //0. name
            if (objResult[i].getValue(columns[0]) != null)
                companyname = objResult[i].getValue(columns[0]);
            else
                companyname = '';
            //1. Id Fiscal
            if (objResult[i].getValue(columns[1]) != null)
                companyruc = objResult[i].getValue(columns[1]);
            else
                companyruc = '';
            //2. Address
            if (objResult[i].getValue(columns[2]) != null)
                companyaddress = objResult[i].getValue(columns[2]);
            else
                companyaddress = '';

        }
    }
    //nlapiLogExecution('DEBUG', 'valores-> ',companyname + ',' + companyruc+ ',' +companyaddress );
    /*}catch(err){
    	sendemail(' [ ObtainSubsidiaria ] ' +err, LMRY_script);
    }*/
    return true;
}
//-------------------------------------------------------------------------------------------------------
//Obtiene a?o y mes del periodo
//-------------------------------------------------------------------------------------------------------
function TraePeriodo(periodo) {

    var mes = '';
    switch (periodo) {
        case '01':
            mes = 'Jan';
            break;
        case '02':
            mes = 'Feb';
            break;
        case '03':
            mes = 'Mar';
            break;
        case '04':
            mes = 'Apr';
            break;
        case '05':
            mes = 'May';
            break;
        case '06':
            mes = 'Jun';
            break;
        case '07':
            mes = 'Jul';
            break;
        case '08':
            mes = 'Aug';
            break;
        case '09':
            mes = 'Sep';
            break;
        case '10':
            mes = 'Oct';
            break;
        case '11':
            mes = 'Nov';
            break;
        case '12':
            mes = 'Dec';
            break;

    }
    //nlapiLogExecution('DEBUG', 'auxmess2-> ',auxmess);
    return mes;
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

function calcular_tasa(tas_)

{
    var tama = result_f.length;
    //nlapiLogExecution('DEBUG', 'result_-> ', tama);
    for (var i = 0; i < tama; i++) {
        if (tas_ == result_f[i].getValue(columnas_f[0])) {
            //nlapiLogExecution('DEBUG', 're-igual-> ', result_f[i].getValue(columnas_f[2]));
            return result_f[i].getValue(columnas_f[2]);
        }
    }

}