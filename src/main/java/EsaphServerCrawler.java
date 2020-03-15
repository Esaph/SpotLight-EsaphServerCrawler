/*
 *  Copyright (C) Esaph, Julian Auguscik - All Rights Reserved
 *  * Unauthorized copying of this file, via any medium is strictly prohibited
 *  * Proprietary and confidential
 *  * Written by Julian Auguscik <esaph.re@gmail.com>, March  2020
 *
 */

import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EsaphServerCrawler
{
	private static final ExecutorService executorMainThread = Executors.newSingleThreadExecutor();
	public static void main(String[] args)
	{
		EsaphServerCrawler.runTimer();
	}
	
	private static void runTimer()
	{
		Calendar today = Calendar.getInstance();
		today.set(Calendar.HOUR_OF_DAY, 2);
		today.set(Calendar.MINUTE, 0);
		today.set(Calendar.SECOND, 0);

		// every night at 2am you run your task
		Timer timer = new Timer();
		timer.schedule(new EsaphServerCrawler.TimerRunner(),
				today.getTime(), 
				TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)); // period: 1 da
	}
	
	private final static class TimerRunner extends TimerTask
	{
		@Override
		public void run()
		{
			EsaphServerCrawler.executorMainThread.execute(new EsaphServerCrawlerSession());
		}
	}
	
}
