/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.index.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import static com.alibaba.innodb.java.reader.Constants.BYTES_OF_INFIMUM;
import static com.alibaba.innodb.java.reader.Constants.BYTES_OF_SUPREMUM;
import static com.alibaba.innodb.java.reader.SizeOf.*;
import static com.google.common.base.Preconditions.checkState;

/**
 * Since MySQL8.0, there is SDI, a.k.a Serialized Dictionary Information(SDI).
 *
 * @author xu.zx
 */
public class SdiPage extends AbstractPage {

  private IndexHeader indexHeader;

  private FsegHeader fsegHeader;

  private RecordHeader infimum;

  private RecordHeader supremum;

  private int[] dirSlots;

  public SdiPage(InnerPage innerPage) {
    super(innerPage);
    this.indexHeader = IndexHeader.fromSlice(sliceInput);
    if (this.indexHeader.getFormat() != PageFormat.COMPACT) {
      reportException();
    }

    // 20 bytes fseg header
    this.fsegHeader = FsegHeader.fromSlice(sliceInput);

    infimum = RecordHeader.fromSlice(sliceInput);
    checkState(Arrays.equals(sliceInput.readByteArray(SIZE_OF_MUM_RECORD), BYTES_OF_INFIMUM));

    supremum = RecordHeader.fromSlice(sliceInput);
    checkState(Arrays.equals(sliceInput.readByteArray(SIZE_OF_MUM_RECORD), BYTES_OF_SUPREMUM));

    int endOfSupremum = sliceInput.position();
    int dirSlotNum = this.indexHeader.getNumOfDirSlots();
    dirSlots = new int[dirSlotNum];
    sliceInput.setPosition(SIZE_OF_PAGE - SIZE_OF_FIL_TRAILER - dirSlotNum * SIZE_OF_PAGE_DIR_SLOT);
    for (int i = 0; i < dirSlotNum; i++) {
      dirSlots[dirSlotNum - i - 1] = sliceInput.readUnsignedShort();
    }
    sliceInput.setPosition(endOfSupremum);
    sliceInput.setPosition(dirSlots[0] + infimum.getNextRecOffset());
    readRecord();

  }

  private String readRecord() {
      sliceInput.decrPosition(5);
      RecordHeader rh = RecordHeader.fromSlice(sliceInput);
      int nexRecord = sliceInput.position() + rh.getNextRecOffset();

      //sliceInput.decrPosition(7);
      sliceInput.decrPosition(6);
      int first = sliceInput.readUnsignedByte();
      int realLength = 0;
      if ((first & 0x80) != 0x00) {
          realLength = (first&0x3f)<<8;
          if ((first & 0x40) != 0x00) {

          } else {
              sliceInput.decrPosition(2);
              int second = sliceInput.readUnsignedByte();
              sliceInput.readUnsignedByte();
              realLength = realLength + second;
          }
      } else {
          realLength = first;
      }
      sliceInput.setPosition(sliceInput.position() + 5);

      sliceInput.setPosition(sliceInput.position() + 25);
      long unzipLength = sliceInput.readUnsignedInt();
      long zipedLength = sliceInput.readUnsignedInt();

      byte[] buffer2 = new byte[(int) zipedLength];
      sliceInput.readBytes(buffer2);
      byte[] buffer3 = decompress(buffer2);
      String g = new String(buffer3);

      sliceInput.setPosition(nexRecord);
      sliceInput.decrPosition(5);
      RecordHeader rh2 = RecordHeader.fromSlice(sliceInput);
      int nexRecord2 = sliceInput.position() + rh2.getNextRecOffset();

      sliceInput.decrPosition(6);
      int ss2 = sliceInput.readUnsignedByte();

      sliceInput.setPosition(nexRecord2);
      sliceInput.decrPosition(5);
      RecordHeader rh3 = RecordHeader.fromSlice(sliceInput);
      byte[] buffer = new byte[1024];
      sliceInput.readBytes(buffer);
      String s = new String(buffer);
      System.out.println(s);
      int b1 = sliceInput.readUnsignedByte();
      if (b1 < 0xf0) {
        System.out.println("s");
      }
      return null;
  }

    public static byte[] decompress(byte[] data) {
        byte[] output = new byte[0];

        Inflater decompresser = new Inflater();
        decompresser.reset();
        decompresser.setInput(data);

        ByteArrayOutputStream o = new ByteArrayOutputStream(data.length);
        try {
            byte[] buf = new byte[2048];
            while (!decompresser.finished()) {
                int i = decompresser.inflate(buf);
                o.write(buf, 0, i);
            }
            output = o.toByteArray();
        } catch (Exception e) {
            output = data;
            e.printStackTrace();
        } finally {
            try {
                o.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        decompresser.end();
        return output;
    }

    private void reportException() throws ReaderException {
    if (this.indexHeader.getIndexId() <= 0L
            && this.indexHeader.getMaxTrxId() <= 0L) {
      throw new ReaderException("Index header is unreadable, only new-style compact page format is supported, "
              + "please make sure the file is a valid InnoDB data file, page="
              + innerPage.toString() + ", index.header = " + this.indexHeader.toString());
    }
    throw new ReaderException("Only new-style compact page format is supported, page=" + innerPage.toString()
            + ", index.header = " + this.indexHeader.toString());
  }

}
