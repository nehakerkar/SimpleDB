package simpledb.buffer;

import simpledb.file.*;
import java.util.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private int glocknumAvailable;
   private int clockPosition;
   private Map<Block, Buffer> bufferPoolMap;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      bufferPoolMap = new HashMap<Block, Buffer>();
      clockPosition = 0;
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer();
      System.out.println("Created "+numAvailable+" buffers.");
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.initRefCount();
         buff.assignToBlock(blk);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      bufferPoolMap.put(blk, buff);
      System.out.println("---------------------------------");
      System.out.println("In pin --> numAvailable:"+(bufferpool.length-bufferPoolMap.size()));
      for (Buffer buff1 : bufferpool) {
          Block b = buff1.block();
          if (b != null)
        	  System.out.print(b+"-->Pin count:"+buff1.pins+", Reference count:"+buff1.referenceValue+" ");
          else
        	  System.out.print("- ");
      }
      System.out.println();
  System.out.println("---------------------------------");
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      bufferPoolMap.put(buff.block(), buff);
      //System.out.println("After PinNew:\nbufferPoolMap : "+bufferPoolMap);
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
    	 buff.initRefCount();
         numAvailable++;
      }
      System.out.println("---------------------------------");
      System.out.println("In pin --> numAvailable:"+(bufferpool.length-bufferPoolMap.size()));
      for (Buffer buff1 : bufferpool) {
          Block b = buff1.block();
          if (b != null)
        	  System.out.print(b+"-->Pin count:"+buff1.pins+", Reference count:"+buff1.referenceValue+" ");
          else
        	  System.out.print("- ");
      }
      System.out.println();
  System.out.println("---------------------------------");
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
	  return bufferPoolMap.get(blk);
   }
   
   private Buffer chooseUnpinnedBuffer() {
	  for (int count = 0;count < 5 ; count++) {
		  for (int count1=0; count1<bufferpool.length; count1++) {
			  if (!bufferpool[clockPosition].isPinned()) {
				  if (!bufferpool[clockPosition].isReferenced()) {
					  System.out.println("GClock policy used, Block replaced: "+bufferpool[clockPosition].block());
					  bufferPoolMap.remove(bufferpool[clockPosition].block());
				          //System.out.println("After Removing.\nbufferPoolMap : "+bufferPoolMap);
					  return bufferpool[clockPosition];
				  }
				  bufferpool[clockPosition].decrRefCount();
			  }
			  clockPosition=((clockPosition+1)%bufferpool.length);
		  }
	  }
      return null;
   }

   /**  
   * Determines whether the map has a mapping from  
   * the block to some buffer.  
   * @param blk the block to use as a key  
   * @return true if there is a mapping; false otherwise  
   */  
   boolean containsMapping(Block blk) {
	   return bufferPoolMap.containsKey(blk);  
   }
   
   /**  
   * Returns the buffer that the map maps the specified block to.  
   * @param blk the block to use as a key  
   * @return the buffer mapped to if there is a mapping; null otherwise  
   */  
   Buffer getMapping(Block blk) {
	   return bufferPoolMap.get(blk);  
   } 

}
