package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;

public class OverflowResourceTest {

	private OverflowResource work;
	private File insertFile;
	private File updateFile;
	private File positionFile;
	private String insertFileName = "unseqTsFile";
	private String updateDeleteFileName = "overflowFile";
	private String positionFileName = "positionFile";
	private String filePath = "overflow";
	private String dataPath = "1";
	private OverflowSupport support = new OverflowSupport();

	@Before
	public void setUp() throws Exception {
		work = new OverflowResource(filePath, dataPath);
		insertFile = new File(new File(filePath, dataPath), insertFileName);
		updateFile = new File(new File(filePath, dataPath), updateDeleteFileName);
		positionFile = new File(new File(filePath, dataPath), positionFileName);
	}

	@After
	public void tearDown() throws Exception {
		work.close();
		support.clear();
		EnvironmentUtils.cleanDir(filePath);
	}

	@Test
	public void testOverflowUpdate() throws IOException {
		OverflowTestUtils.produceUpdateData(support);
		work.flush(null, null, support.getOverflowSeriesMap(), "processorName");
		work.appendMetadatas();
		List<TimeSeriesChunkMetaData> chunkMetaDatas = work.getUpdateDeleteMetadatas(OverflowTestUtils.deltaObjectId1,
				OverflowTestUtils.measurementId1, OverflowTestUtils.dataType2);
		assertEquals(true, chunkMetaDatas.isEmpty());
		chunkMetaDatas = work.getUpdateDeleteMetadatas(OverflowTestUtils.deltaObjectId1,
				OverflowTestUtils.measurementId1, OverflowTestUtils.dataType1);
		assertEquals(1, chunkMetaDatas.size());
		TimeSeriesChunkMetaData chunkMetaData = chunkMetaDatas.get(0);
		assertEquals(OverflowTestUtils.dataType1, chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType());
		assertEquals(OverflowTestUtils.measurementId1, chunkMetaData.getProperties().getMeasurementUID());
		// close
		work.close();
		// append file
		long originlength = updateFile.length();
		FileOutputStream fileOutputStream = new FileOutputStream(updateFile, true);
		fileOutputStream.write(new byte[20]);
		fileOutputStream.close();
		assertEquals(originlength + 20, updateFile.length());
		work = new OverflowResource(filePath, dataPath);
		chunkMetaDatas = work.getUpdateDeleteMetadatas(OverflowTestUtils.deltaObjectId1,
				OverflowTestUtils.measurementId1, OverflowTestUtils.dataType2);
		assertEquals(true, chunkMetaDatas.isEmpty());
		chunkMetaDatas = work.getUpdateDeleteMetadatas(OverflowTestUtils.deltaObjectId1,
				OverflowTestUtils.measurementId1, OverflowTestUtils.dataType1);
		assertEquals(1, chunkMetaDatas.size());
		chunkMetaData = chunkMetaDatas.get(0);
		assertEquals(OverflowTestUtils.dataType1, chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType());
		assertEquals(OverflowTestUtils.measurementId1, chunkMetaData.getProperties().getMeasurementUID());
		assertEquals(originlength, updateFile.length());
	}

	@Test
	public void testOverflowInsert() throws IOException {
		OverflowTestUtils.produceInsertData(support);
		work.flush(OverflowTestUtils.getFileSchema(), support.getMemTabale(), null, "processorName");
		List<TimeSeriesChunkMetaData> chunkMetaDatas = work.getInsertMetadatas(OverflowTestUtils.deltaObjectId1,
				OverflowTestUtils.measurementId1, OverflowTestUtils.dataType2);
		assertEquals(0, chunkMetaDatas.size());
		work.appendMetadatas();
		chunkMetaDatas = work.getInsertMetadatas(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1,
				OverflowTestUtils.dataType1);
		assertEquals(1, chunkMetaDatas.size());
		TimeSeriesChunkMetaData chunkMetaData = chunkMetaDatas.get(0);
		assertEquals(OverflowTestUtils.dataType1, chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType());
		assertEquals(OverflowTestUtils.measurementId1, chunkMetaData.getProperties().getMeasurementUID());
		// close
		work.close();
		// append file
		long originlength = insertFile.length();
		FileOutputStream fileOutputStream = new FileOutputStream(insertFile, true);
		fileOutputStream.write(new byte[20]);
		fileOutputStream.close();
		assertEquals(originlength + 20, insertFile.length());
		work = new OverflowResource(filePath, dataPath);
		chunkMetaDatas = work.getInsertMetadatas(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1,
				OverflowTestUtils.dataType1);
		assertEquals(1, chunkMetaDatas.size());
		chunkMetaData = chunkMetaDatas.get(0);
		assertEquals(OverflowTestUtils.dataType1, chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType());
		assertEquals(OverflowTestUtils.measurementId1, chunkMetaData.getProperties().getMeasurementUID());
		assertEquals(originlength, insertFile.length());
	}
}
