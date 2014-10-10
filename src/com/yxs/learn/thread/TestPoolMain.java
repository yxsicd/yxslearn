package com.yxs.learn.thread;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestPoolMain {

	final static ScheduledExecutorService myPool = Executors
			.newSingleThreadScheduledExecutor();
	final static ExecutorService downpool = Executors.newCachedThreadPool();

	public static void main(String[] args) {


		
		myPool.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				System.out.println("-------------------------------");
				downloadPool();
				try {
					Thread.sleep(5000);
					System.out.println("===============================");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, 0, 1, TimeUnit.SECONDS);
		
		
		while(true){}

	}

	private static void downloadPool() {
		List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
		tasks.add(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				System.out.println("begin1");
				Thread.sleep(50000);
				System.out.println("done1");
				return true;
			}
		});

		tasks.add(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				System.out.println("begin2");
				System.out.println("done2");
				throw new Exception();
			}
		});

		tasks.add(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				System.out.println("begin3");
				System.out.println("done3");
				return true;
			}
		});

		long timeout = 2;
		TimeUnit unit = TimeUnit.SECONDS;
		try {
			downpool.invokeAll(tasks, timeout, unit);
			System.out.println("time out1");

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
