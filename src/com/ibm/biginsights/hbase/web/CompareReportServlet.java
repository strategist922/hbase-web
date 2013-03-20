package com.ibm.biginsights.hbase.web;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.ibm.biginsights.sda.ingest.util.FileUtil;

/**
 * Servlet implementation class CompareReportServlet
 * 
 * @author Julian zhouxbj@cn.ibm.com
 */
@WebServlet(description = "Compare the data set and geenrate report.", urlPatterns = { "/CompareReportServlet" })
public class CompareReportServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  public static float odd_value_threshold = 10;

  /**
   * @see HttpServlet#HttpServlet()
   */
  public CompareReportServlet() {
    super();
    // TODO Auto-generated constructor stub
  }

  /**
   * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String reqOddThreshold = request.getParameter("oddth");
    String freq4PointStr = request.getParameter("freq");

    // set frequency of data point for chat, default is 1
    int freq4Point = 1;
    if (freq4PointStr != null && !freq4PointStr.equals("")) {
      freq4Point = Integer.parseInt(freq4PointStr);
    }

    // set threshold for odd value
    if (reqOddThreshold != null && !reqOddThreshold.equals("")) {
      odd_value_threshold = Float.parseFloat(reqOddThreshold);
    }

    // prepare the print writer for response
    response.setContentType("text/html;charset=utf-8");
    PrintWriter pw = response.getWriter();

    // get server file root path
    String fileRootPath = request.getServletContext().getRealPath("/");

    // Data for comparing are in original.txt and new.txt under /data
    String oriDataFilePath = fileRootPath + File.separator + "data"
        + File.separator + "original.txt";
    String newDataFilePath = fileRootPath + File.separator + "data"
        + File.separator + "new.txt";
    File oriDataFile = new File(oriDataFilePath);
    File newDataFile = new File(newDataFilePath);

    JSONArray jsonArray = new JSONArray();
    JSONObject jsonObj = new JSONObject();

    if (!oriDataFile.exists() && !newDataFile.exists()) {
      jsonObj
          .put(
              "error",
              "[ERROR]: original and new data files for comparing do not exist. You may need to run a pass for both.");
    } else if (!oriDataFile.exists()) {
      jsonObj
          .put(
              "error",
              "[ERROR]: original data file for comparing does not exist. You may need to run original pass.");
    } else if (!newDataFile.exists()) {
      jsonObj
          .put(
              "error",
              "[ERROR]: new data file for comparing does not exist. You may need to run new pass.");
    } else {

      // read original and new data, data points are seperated by ","
      String oriData = FileUtil.readFile(oriDataFilePath, null);
      String newData = FileUtil.readFile(newDataFilePath, null);

      String[] oriDataPts = oriData.split(",");
      String[] newDataPts = newData.split(",");

      // data records
      float diff = 0;
      float diffTotal = 0;
      float diffAvg = 0;
      long oddPointCnt = 0;
      long readPointCnt = 0;
      long totalPointCnt = oriDataPts.length;

      StringBuffer diffArrayStr = new StringBuffer("[");
      int cnt4Freq = freq4Point;
      float freqReadPointTotal = 0;

      JSONArray arrayOfDataRec = new JSONArray();

      // for each data point compare between original and new
      for (int i = 0; i < totalPointCnt; i++) {
        diff = (Float.parseFloat(newDataPts[i]) - Float
            .parseFloat(oriDataPts[i])) / Float.parseFloat(oriDataPts[i]) * 100;

        // count odd/normal point and total for final summary calculation
        if (Math.abs(diff) > odd_value_threshold) {
          oddPointCnt++;
        } else {
          readPointCnt++;
          diffTotal += diff;
        }

        // add a data compare record
        JSONObject dataRec = new JSONObject();
        dataRec.put("original", oriDataPts[i]);
        dataRec.put("new", newDataPts[i]);
        dataRec.put("diff", Float.toString(diff));
        arrayOfDataRec.add(dataRec);

        // add average difference point for chart based on requested frequency
        cnt4Freq--;
        freqReadPointTotal += diff;
        if (cnt4Freq == 0) {

          // add average difference point and reset for the next one
          diffArrayStr.append(Float.toString(freqReadPointTotal / freq4Point)
              + ",");
          cnt4Freq = freq4Point;
          freqReadPointTotal = 0;
        }
      }

      diffArrayStr.deleteCharAt(diffArrayStr.lastIndexOf(","));
      diffArrayStr.append("]");

      jsonObj.put("records", arrayOfDataRec);
      jsonObj.put("diffarray", diffArrayStr.toString());

      // summary
      diffAvg = diffTotal / readPointCnt;
      StringBuffer summaryStr = new StringBuffer();
      summaryStr.append("&nbsp;&nbsp;&nbsp;&nbsp;============================<br>");
      summaryStr.append("&nbsp;&nbsp;&nbsp;&nbsp;Total read pass for comparison: " + totalPointCnt
          + "<br>");
      summaryStr.append("&nbsp;&nbsp;&nbsp;&nbsp;Counted read pass for comparison: " + readPointCnt
          + "<br>");
      summaryStr.append("&nbsp;&nbsp;&nbsp;&nbsp;Odd read pass due to noise: " + oddPointCnt
          + ". Diff threshold as odd is +-" + odd_value_threshold + "%. <br>");
      summaryStr.append("&nbsp;&nbsp;&nbsp;&nbsp;Diff average: <font color='red'>" + Double.toString(diffAvg) + "%</font>");

      jsonObj.put("summary", summaryStr.toString());

    }

    jsonArray.add(jsonObj);
    pw.write(jsonArray.toString());
    pw.close();
  }

  /**
   * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // TODO Auto-generated method stub
  }

}
