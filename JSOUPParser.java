package com.pointcarbon.parsers.FLOW;

import com.pointcarbon.esb.commons.beans.EsbMessage;
import com.pointcarbon.parserframework.service.IParser;
import com.pointcarbon.parserframework.service.OutputServiceFactory;
import com.pointcarbon.parserframework.service.output.STFDataOutputter;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.pointcarbon.parserframework.service.output.STFDataOutputter.COLUMN.*;

public class JSOUPParser implements IParser {
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("CET");
    private static final DateTimeFormatter DTF1 = DateTimeFormat.forPattern("dd MMM yyyyHH:mm").withZone(TIME_ZONE);
    private static final String DTF2 = "MM/dd/yyyy/HH:mm";
    private final static String REGEX_TIME_STAMP = "_dateTime_(\\d{2}-\\d{2}-\\d{4}-\\d{2}_\\d{2}\\+\\d{4})_\\w*.txt";

    private final static String FRE_DISCH_PORT_NAME_MOVEMENTS = "Brunsbüttel - Port of Hamburg";
    private final static String FRE_DISCH_POINT_TYPE_MOVEMENTS = "Terminal";
    private final static String FRE_ARR_DATE_FCST_ACTL_MOVEMENTS = "ACTUAL";
    private final static String FRE_STATUS_MOVEMENTS = "In Port";
    private final static String FRE_ARR_DATE_TO_MOVEMENTS = "";
    private final static String FRE_COMMENTS_PRIVATE_TYPE = "Unknown Operation";
    private final static String MOVEMENTS_TYPE = "MOVEMENTS";

    private final static String FRE_DISCH_PORT_NAME_EXPECTED = "Brunsbüttel - Port of Hamburg";
    private final static String FRE_DISCH_POINT_TYPE_EXPECTED = "Terminal";
    private final static String FRE_ARR_DATE_FCST_ACTL_EXPECTED = "FORECAST";
    private final static String FRE_STATUS_EXPECTED = "Expected";
    private final static String EXPECTED_TYPE = "EXPECTED";

    private final static String COMMENTS_PART_1 = "Last time in port:%DATETIME%;Terminal:%PORTNAME%";
    private final static String COMMENTS_PART_2 = ";Next Port:%NEXTPORT%";
    private final static String LAST_TYPE = "last";
    private final static String JSOUP_PATH = "tr > td";


    @Override
    public void parse(InputStream is, EsbMessage message, OutputServiceFactory factory) throws Exception {
        STFDataOutputter outputter = factory.createSTFDataOutputter();
        ZipArchiveInputStream zipIs = new ZipArchiveInputStream(new BufferedInputStream(is));
        ZipArchiveEntry zipEntry = zipIs.getNextZipEntry();

        while (zipEntry != null) {
            String html = IOUtils.toString(zipIs, StandardCharsets.UTF_8);
            Document document = Jsoup.parse(html);
            String fileName = zipEntry.getName();
            Element table = document.select("table").get(0);
            String vesselName = document.getElementsByTag("h1").html();
            String vesselType = table.getElementsByTag("p").get(0).html();
            String imo = table.getElementsByTag("td").get(1).html();
            String timeStamp = getTimeStamp(fileName);
            String freeComents = "";
            Element portInfo = document.getElementById("port_vessel");
            portInfo.select("tr").get(0).remove();
            Elements tableElements = portInfo.select("tr");

            LinkedHashMap<String, String> orderedContent = getOrderedMap(tableElements);
            for (Map.Entry<String, String> content : orderedContent.entrySet()) {
                String prevPort = "";
                String arrivalOrLast = "";
                String portLastOrArr = "";
                String dateTimeDeparture = "";
                String dateTimeLastOrArr = "";

                if (!content.getKey().isEmpty()) {
                    String[] lastOrArr = content.getKey().split(",");
                    arrivalOrLast = lastOrArr[1];
                    dateTimeLastOrArr = getDateTime(lastOrArr);
                    if (lastOrArr.length == 4) {
                        vesselName = vesselName.replace("Internal Server Error", "").replace("\n", "");
                    } else {
                        portLastOrArr = lastOrArr[4];
                    }
                }

                if (!content.getValue().isEmpty()) {
                    String[] departure = content.getValue().split(",");
                    dateTimeDeparture = getDateTime(departure);
                }

                if (arrivalOrLast.equalsIgnoreCase(LAST_TYPE) && !portLastOrArr.isEmpty() && !dateTimeLastOrArr.isEmpty()) {
                    freeComents = getFreeComments(portLastOrArr, dateTimeLastOrArr, document);
                }
                prevPort = getPrevPort(document);

                sendLineup(outputter, fileName, freeComents, timeStamp, portLastOrArr,
                        dateTimeLastOrArr, vesselName, imo, vesselType, dateTimeDeparture, arrivalOrLast, prevPort);

                if (fileName.contains(MOVEMENTS_TYPE))
                    break;
            }
            zipEntry = zipIs.getNextZipEntry();
        }
    }

    private void lineupSample(STFDataOutputter outputter, String timeStamp, String freDischPortName, String terminal, String freDischPointType,
                              String dateTime, String freeArrDateTo, String freeArrDateFcstActl, String freeStatus, String vesselName,
                              String imo, String vesselType, String freeComments, String commentsPrivate, String prevPort) throws IOException {
        try {
            outputter.commit(timeStamp, FLOW_CREATE_DATE);
            outputter.commit(freDischPortName, FRE_DISCH_PORT_NAME);
            outputter.commit(terminal, FRE_DISCH_POINT_NAME);
            outputter.commit(freDischPointType, FRE_DISCH_POINT_TYPE);
            outputter.commit(dateTime, FRE_ARR_DATE_FROM);
            outputter.commit(freeArrDateTo, FRE_ARR_DATE_TO);
            outputter.commit(freeArrDateFcstActl, FRE_ARR_DATE_FCST_ACTL);
            outputter.commit(freeStatus, FRE_STATUS);
            outputter.commit(vesselName, FRE_VES_NAME);
            outputter.commit(imo, FRE_VES_IMO);
            outputter.commit(vesselType, FRE_VES_TYPE);
            outputter.commit(freeComments, FRE_COMMENTS);
            outputter.commit(commentsPrivate, FRE_COMMENTS_PRIVATE);
            outputter.commit(prevPort, FLOW_LOAD_LOCATION);
        } catch (Exception e) {
            outputter.rollabck();
        }
        outputter.push();
    }

    public void sendLineup(STFDataOutputter outputter, String fileName, String freeComents, String timeStamp,
                           String portLastOrArr, String dateTimeLastOrArr, String vesselName, String imo,
                           String vesselType, String dateTimeDeparture, String checkType, String prevPort) throws IOException {
        if (fileName.contains(MOVEMENTS_TYPE)) {
            if (!freeComents.contains("Next Port"))
                freeComents = "";
            lineupSample(outputter, timeStamp, FRE_DISCH_PORT_NAME_MOVEMENTS, portLastOrArr, FRE_DISCH_POINT_TYPE_MOVEMENTS,
                    dateTimeLastOrArr, FRE_ARR_DATE_TO_MOVEMENTS, FRE_ARR_DATE_FCST_ACTL_MOVEMENTS, FRE_STATUS_MOVEMENTS,
                    vesselName, imo, vesselType, freeComents, FRE_COMMENTS_PRIVATE_TYPE, prevPort);
        } else if (fileName.contains(EXPECTED_TYPE)) {
            if (!checkType.equalsIgnoreCase(LAST_TYPE))
                lineupSample(outputter, timeStamp, FRE_DISCH_PORT_NAME_EXPECTED, portLastOrArr, FRE_DISCH_POINT_TYPE_EXPECTED,
                        dateTimeLastOrArr, dateTimeDeparture, FRE_ARR_DATE_FCST_ACTL_EXPECTED, FRE_STATUS_EXPECTED,
                        vesselName, imo, vesselType, freeComents, FRE_COMMENTS_PRIVATE_TYPE, prevPort);
        }
    }

    private LinkedHashMap<String, String> getOrderedMap(Elements tableElements) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < tableElements.size(); i++) {
            Element element = tableElements.get(i);
            Elements elementsContent = element.select(JSOUP_PATH);
            int countTagP = element.select(JSOUP_PATH).get(0).childrenSize();
            if (countTagP < 2) {
                String oneRow = concatArOrDev(0, elementsContent, countTagP);
                map.put(oneRow, "");
            } else if (countTagP == 2) {
                String arrival = concatArOrDev(0, elementsContent, countTagP);
                String departure = concatArOrDev(1, elementsContent, countTagP);
                map.put(arrival, departure);
            }
        }
        return map;
    }

    private String concatArOrDev(int indexElement, Elements elements, int oneRow) {
        String concatRow = "";
        for (Element element : elements) {
            if (oneRow < 2) {
                concatRow = concatRow + "," + element.text();
            } else {
                if (element.toString().contains("</p>")) {
                    concatRow = concatRow + "," + element.select("p").get(indexElement).text();
                } else if (element.toString().contains("</a>")) {
                    concatRow = concatRow + "," + element.select("a").text();
                } else {
                    concatRow = concatRow + "," + element.text();
                }
            }
        }
        return concatRow;
    }

    private String getFreeComments(String portName, String dateTime, Document document) {
        String comments = COMMENTS_PART_1.replace("%DATETIME%", dateTime).replace("%PORTNAME%", portName);
        if (document.toString().contains("port_next_prev")) {
            String nextPort = document.getElementById("port_next_prev").select(JSOUP_PATH).get(1).text();
            comments = comments + COMMENTS_PART_2.replace("%NEXTPORT%", nextPort);
        }
        return comments;
    }

    private String getPrevPort(Document document) {
        if (document.toString().contains("port_next_prev")) {
            return document.getElementById("port_next_prev").select(JSOUP_PATH).get(0).text();
        }
        return "";
    }

    private String getTimeStamp(String document) {
        Pattern pattern = Pattern.compile(REGEX_TIME_STAMP);
        Matcher matcher = pattern.matcher(document);
        if (matcher.find()) {
            return matcher.group(1).replaceAll("-", "/").replace("_", ":");
        }
        return "";
    }

    private String getDateTime(String[] array) {
        String dateTimeCreate = array[2] + array[3];
        if (dateTimeCreate.matches("\\d{1,2}.\\w*.\\d{4}\\d{2}:\\d{2}")) {
            return DTF1.parseDateTime(dateTimeCreate).toString(DTF2);
        }
        return "";
    }

}