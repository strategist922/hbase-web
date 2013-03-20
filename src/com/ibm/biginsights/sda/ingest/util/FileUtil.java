/**
 * 
 */
package com.ibm.biginsights.sda.ingest.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * File operation utilities.
 * @author Julian zhouxbj@cn.ibm.com 
 */
public class FileUtil {

	private static final String SPLIT_FILE_SUFFIX = ".split";

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// File dataRootDir = new File("E:\\ws_weibo\\sda_zh_dyn_web\\data\\");
		// File[] dataFileDirs = dataRootDir.listFiles();
		// for (File dataFileDir : dataFileDirs) {
		// mergeData(dataFileDir.getAbsolutePath());
		// }

		// processABatchOutput();

		// wrapDict2JSONArray(
		// "E:\\Big Data\\IBM\\Showcases_Demo\\SDA_zh\\web\\dictionary\\taobao\\笔记本电脑\\型号.dict",
		// "E:\\Big Data\\IBM\\Showcases_Demo\\SDA_zh\\web\\dictionary\\taobao\\笔记本电脑\\model.json",
		// "item");

		wrap2JSONArrayFile("E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupNSentimentByBrandUserList.json\\");
		wrap2JSONArrayFile("E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupPSentimentByBrandUserList.json\\");
		wrap2JSONArrayFile("E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupQualityByBrandUserList.json\\");
		wrap2JSONArrayFile("E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupIntentByBrandUserList.json\\");
	}

	/**
	 * Process a batch output for all dimension data.
	 */
	public static void processABatchOutput() {
		wrap2JSONArrayFile("E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\");

		splitJsonByEntity(
				"attribute",
				"E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupByAttributeDetails.json\\part-00000",
				"E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupByAttributeDetails.json\\part-00000.split");
		splitJsonByEntity(
				"modelNumber",
				"E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupByModelNumberDetails.json\\part-00000",
				"E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupByModelNumberDetails.json\\part-00000.split");
		splitJsonByEntity(
				"brand",
				"E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupByBrandDetails.json\\part-00000",
				"E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupByBrandDetails.json\\part-00000.split");

		splitJsonFilesByPage(
				"brand",
				"E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupByBrandDetails.json\\part-00000.split",
				10);
		splitJsonFilesByPage(
				"modelNumber",
				"E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupByModelNumberDetails.json\\part-00000.split",
				10);
		splitJsonFilesByPage(
				"attribute",
				"E:\\ws_weibo\\sda_zh_dyn_web\\WebContent\\data\\2011-01\\groupByAttributeDetails.json\\part-00000.split",
				10);
	}

	/**
	 * Replace the string in file name for a file or files under directory
	 * recursively.
	 * 
	 * @param file
	 * @param orig
	 * @param target
	 */
	public static void replaceDirFileNameString(String file, String orig,
			String target) {

		File aFile = new File(file);
		String newFileName = aFile.getName().replace(orig, target);
		System.out.println(aFile.renameTo(new File(aFile.getParent()
				+ File.separator + newFileName)));

		if (aFile.isDirectory()) {
			String[] fileArray = aFile.list();
			for (String subFile : fileArray) {
				replaceDirFileNameString(aFile + File.separator + subFile,
						orig, target);
			}
		}
	}

	/**
	 * Read the file to a output string with or without delimiter at the end of
	 * each line.
	 * 
	 * @param filePath
	 * @param splitter
	 *            if null, then no delimiter at the end of each line
	 * @return
	 */
	public static String readFile(String filePath, String splitter) {
		FileInputStream f = null;
		BufferedReader br = null;
		StringBuffer sb = new StringBuffer();
		String aLine = null;

		try {
			f = new FileInputStream(filePath);
			br = new BufferedReader(new InputStreamReader(f, "UTF-8"));
			aLine = br.readLine();
			if (splitter == null) {
				while (aLine != null) {
					sb.append(aLine);
					aLine = br.readLine();
				}
			} else {
				while (aLine != null) {
					sb.append(aLine.trim() + splitter);
					aLine = br.readLine();
				}
				sb.delete(sb.length() - splitter.length(), sb.length());
			}
			br.close();
			f.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return sb.toString();
	}

	/**
	 * Remove a line in a file. No blank line left.
	 * 
	 * @param file
	 * @param lineNum
	 */
	public static void removeALine(String file, int lineNum) {
		File aFile = new File(file);

		if (aFile.isDirectory()) {
			File[] fileArray = aFile.listFiles();
			for (File subFile : fileArray) {
				removeALine(subFile.getAbsolutePath(), lineNum);
			}
		} else {
			int num = 0;
			try {
				BufferedReader br = new BufferedReader(new FileReader(
						aFile.getAbsolutePath()));
				String str = null;
				List list = new ArrayList();
				while ((str = br.readLine()) != null) {
					++num;
					System.out.println(num + "行：" + str);
					if (num == lineNum)
						continue;
					list.add(str);
				}
				br.close();

				System.out.println("list size:" + list.size());
				BufferedWriter bw = new BufferedWriter(new FileWriter(
						aFile.getAbsolutePath()));
				for (int i = 0; i < list.size(); i++) {
					System.out.println("list[" + i + "]" + list.get(i));
					bw.write(list.get(i).toString());
					bw.newLine();
				}
				bw.flush();
				bw.close();
				System.out.println("删除成功");

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	/**
	 * Wrap a file or files under directory to JSON array format. Suppose the
	 * content in files are already JSON Array or JSON Object list.
	 * 
	 * @param filePath
	 */
	public static void wrap2JSONArrayFile(String filePath) {

		File aFile = new File(filePath);
		if (aFile.isDirectory()) {
			// recursive call for files in the directory
			File[] fileArray = aFile.listFiles();
			for (File subFile : fileArray) {
				wrap2JSONArrayFile(subFile.getAbsolutePath());
			}
		} else {
			System.out.println(filePath);
			// skip the file without file name pattern "part-00000"
			if (!filePath.contains("part-00000")) {
				return;
			}

			FileInputStream f = null;
			BufferedReader br = null;
			StringBuffer sb = new StringBuffer();
			String aLine = null;

			try {
				f = new FileInputStream(filePath);
				br = new BufferedReader(new InputStreamReader(f));
				aLine = br.readLine();
				sb.append("[");
				while (aLine != null) {
					if (aLine.trim().startsWith("{")) {
						sb.append(aLine.trim() + ",");
					}
					aLine = br.readLine();
				}
				sb.deleteCharAt(sb.lastIndexOf(","));
				sb.append("]");

				br.close();
				f.close();

				FileWriter fw = new FileWriter(filePath);
				fw.write(sb.toString());
				fw.close();

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	/**
	 * Split json to multiple jsons by entity to a target directory.
	 * 
	 * @param entityName
	 * @param targetJsonFile
	 * @param desDir
	 */
	public static void splitJsonByEntity(String entityName,
			String targetJsonFile, String desDir) {

		File dir = new File(desDir);
		dir.mkdir();

		try {
			JSONArray jsonArray = JSONArray.fromObject(readFile(targetJsonFile,
					null));

			// split the json array by every entity name
			for (Iterator iterator = jsonArray.iterator(); iterator.hasNext();) {
				JSONObject object = (JSONObject) iterator.next();
				FileWriter fw = new FileWriter(desDir + File.separator
						+ object.getString(entityName) + ".json");
				fw.write("[" + object.toString() + "]");
				fw.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Split json files under directory by page. Each page gets the specific
	 * snippet unit.
	 * 
	 * @param entityName
	 * @param jsonFileDir
	 * @param snippetUnit
	 */
	public static void splitJsonFilesByPage(String entityName,
			String jsonFileDir, int snippetUnit) {
		File dir = new File(jsonFileDir);
		if (dir.isDirectory()) {
			File[] fileArray = dir.listFiles();
			for (File subFile : fileArray) {
				splitAJsonFileByPage(entityName, subFile.getAbsolutePath(),
						snippetUnit);
			}
		}
	}

	/**
	 * Split a json file by page. Each page gets the specific snippet unit.
	 * 
	 * @param entityName
	 * @param targetJsonFile
	 * @param snippetUnit
	 */
	public static void splitAJsonFileByPage(String entityName,
			String targetJsonFile, int snippetUnit) {

		// create directory for all snippet json files
		String targetSnippetDir = targetJsonFile + SPLIT_FILE_SUFFIX;
		File dir = new File(targetSnippetDir);
		dir.mkdir();

		// get the detailed record json array for this entity, and count the
		// number of records
		JSONArray jsonArray = JSONArray.fromObject(readFile(targetJsonFile,
				null));
		JSONArray jsonArrayForSplit = ((JSONObject) jsonArray.get(0))
				.getJSONArray("details");
		String itemName = ((JSONObject) jsonArray.get(0)).getString(entityName);
		int jsonObjectCnt = jsonArrayForSplit.size();

		// calculate the total page number
		int totalPageCnt = (jsonObjectCnt % snippetUnit) > 0 ? (jsonObjectCnt
				/ snippetUnit + 1) : (jsonObjectCnt / snippetUnit);

		// initialize the current page and record counter for each page
		int currentPage = 1;
		int tempCnt = 0;

		// split the records in details into snippet
		FileWriter fw = null;
		JSONObject snippetJsonObj = null;
		JSONArray snippetJsonArray = null;

		try {
			for (Iterator iterator = jsonArrayForSplit.iterator(); iterator
					.hasNext();) {
				JSONObject object = (JSONObject) iterator.next();

				if (tempCnt == 0) {

					// this is the 1st record for each new snippet
					fw = new FileWriter(targetSnippetDir + File.separator
							+ itemName + SPLIT_FILE_SUFFIX + currentPage
							+ ".json");
					fw.write("[");
					snippetJsonObj = new JSONObject();
					// e.g. "brand" : "Apple"
					snippetJsonObj.put(entityName, itemName);
					// e.g. "totalcount" : "100"
					snippetJsonObj.put("totalcount", jsonObjectCnt);
					// e.g. "pagecunt" : "5"
					snippetJsonObj.put("pagecount", totalPageCnt);
					// e.g. "page" : "1"
					snippetJsonObj.put("page", currentPage++);

					snippetJsonArray = new JSONArray();
					snippetJsonArray.add(object);

					tempCnt++;

				} else {
					// continuously add record to current snippet
					snippetJsonArray.add(object);
					tempCnt++;

					// if snippet unit limit met, then write it out
					if (tempCnt == snippetUnit) {
						snippetJsonObj.put("count", tempCnt);
						snippetJsonObj.put("details", snippetJsonArray);
						fw.write(snippetJsonObj.toString());
						fw.write("]");
						fw.close();

						tempCnt = 0;
					}
				}

			} // for

			// if temp counter is not 0, then means left records need to sit in
			// the last snippet
			if (tempCnt > 0) {
				snippetJsonObj.put("count", tempCnt);
				snippetJsonObj.put("details", snippetJsonArray);
				fw.write(snippetJsonObj.toString());
				fw.write("]");
				fw.close();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Select first 10 lines of data to merge to a single file "seg" under a
	 * given data file directory. This is for downsizing the data for demo.
	 * 
	 * @param dataFileDir
	 */
	public static void mergeData(String dataFileDir) {

		try {
			// prepare to write the merge data file
			FileWriter fw = new FileWriter(dataFileDir + File.separator + "seg");

			File dir = new File(dataFileDir);

			int lineCnt = 0;
			File[] dataFiles = dir.listFiles();
			for (File dataFile : dataFiles) {

				FileInputStream f = new FileInputStream(
						dataFile.getAbsolutePath());
				BufferedReader br = new BufferedReader(new InputStreamReader(f));
				String aLine = br.readLine(); // skip the first line of keywords
				aLine = br.readLine();
				while (aLine != null && lineCnt < 10) {
					fw.write(aLine);
					fw.write("\r");
					lineCnt++;
					aLine = br.readLine();
				}
				lineCnt = 0;

				br.close();
				f.close();
			}

			fw.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Wrap plain dictionary file to a json array format file.
	 * 
	 * @param srcFile
	 * @param desFile
	 * @param itemLabel
	 */
	public static void wrapDict2JSONArray(String srcFile, String desFile,
			String itemLabel) {
		try {
			JSONArray jsonArray = new JSONArray();

			FileInputStream f = new FileInputStream(srcFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(f));
			String aLine = br.readLine();
			while (aLine != null) {
				JSONObject jsonObj = new JSONObject();
				jsonObj.put(itemLabel, aLine.trim());
				jsonArray.add(jsonObj);
				aLine = br.readLine();
			}

			br.close();
			f.close();

			// prepare to write the merge data file
			FileWriter fw = new FileWriter(desFile);
			fw.write(jsonArray.toString());
			fw.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
