/*
 * @(#)	Apr 24, 2015
 * Copyright (c) 2015 @wutalk on github. All rights reserved.
 */
package com.leansoft.bigqueue.tutorial;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

/**
 * 
 * @author wutalk
 */
public class BigQueueLearn {

	public static void main(String[] args) {
		IBigQueue bigQueue = null;
		// 32M
		int pageSize = 32 * 1024 * 1024;
		try {
			bigQueue = new BigQueueImpl("F:/devlab/bigqueue/samples", "dn_find", pageSize);

			bigQueue.enqueue("item".getBytes());
			
			while (true) {
				byte[] data = bigQueue.dequeue();
				if (data == null)
					break;
				System.out.println("got dn: " + new String(data));
			}
			// now the big is empty since all items have been dequeued
			assertTrue(bigQueue.isEmpty());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// release resources
			try {
				bigQueue.close();
				// delete page files
				// bigQueue.removeAll();
				System.out.println("done");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
