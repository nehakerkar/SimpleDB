
package simpledb.tx.recovery;

import simpledb.server.SimpleDB;
import simpledb.buffer.*;
import simpledb.file.Block;
import simpledb.log.BasicLogRecord;

public class UpdateRecord implements LogRecord {
	private int txnum, offset, blockNum;
	private String filename;
	//private Block blk;
	/**
	 * Handles the updates on the record.
	 **/
	public UpdateRecord(int txnum,  String filename, int BlockNum, int offset) {
		this.txnum = txnum;
		this.blockNum = BlockNum;
		this.offset = offset;
		this.filename = filename;
	}
	/**
	 * Update a log record by editing the four values.
	 **/
	public UpdateRecord(BasicLogRecord rec) {
		txnum = rec.nextInt();
		//blk= new Block(filename, blockNum);
		filename = rec.nextString();
		blockNum = rec.nextInt();
		offset = rec.nextInt();
	}
	public int writeToLog() {
		Object[] rec = new Object[] {UPDATE, this.txnum, this.filename, this.blockNum , this.offset};
		return logMgr.append(rec);
	}
	public int op() {
		return UPDATE;
	}
	public int txNumber() {
		return txnum;
	}
	public String toString() {
		return "<UPDATE " + this.txnum + " " + this.filename + " " + this.blockNum + " " + this.offset  + ">";
	}
	// Replaces the specified data value with the value saved in the log record.
	public void undo(int txnum) {
		BufferMgr buffMgr = SimpleDB.bufferMgr();
		Block blk= new Block("SavedBlocks.txt", offset);
		Buffer buff = buffMgr.pin(blk);
		buff.restoreBlock(offset);
		buffMgr.unpin(buff);
	}
}
