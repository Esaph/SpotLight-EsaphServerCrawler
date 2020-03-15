/*
 *  Copyright (C) Esaph, Julian Auguscik - All Rights Reserved
 *  * Unauthorized copying of this file, via any medium is strictly prohibited
 *  * Proprietary and confidential
 *  * Written by Julian Auguscik <esaph.re@gmail.com>, March  2020
 *
 */

import java.io.File;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

public class EsaphServerCrawlerSession extends Thread
{
	private static final String placeholder = "EsaphServerCrawlerSession: ";
	public EsaphServerCrawlerSession()
	{
	}
	
	private static final String CONNECTION = "jdbc:mysql://localhost/LifeCapture?useSSL=false";
	private static final String SQLUser = "FServer";
	private static final String SQLPass = "dHlEZ\"4fop$Os5\"Ynw*linZ4NerQXTTBjTrk";
	private Connection connection = null;
	
	
	//QUERYS
	private static final String queryGetPrivatePostsPassed24Hours =
			"SELECT PID, PPID FROM PrivateMoments WHERE Time <= DATE_SUB(NOW(), INTERVAL 24 HOUR) AND NOT EXISTS(SELECT NULL FROM PrivateMomentsSaved WHERE PrivateMoments.PPID = PrivateMomentsSaved.PPID) LIMIT ?, 100";

	private static final String queryRemovePostingPrivateMoments = "DELETE FROM PrivateMoments WHERE PPID=?";
	private static final String queryRemoveHashtagsFromPost = "DELETE FROM TAGS WHERE PPID=?";

	private Connection createNewConnectionForPool()
	{
		Connection connection = null;

		try
		{
			connection = (Connection) DriverManager.getConnection(
					EsaphServerCrawlerSession.CONNECTION, 
					EsaphServerCrawlerSession.SQLUser, 
					EsaphServerCrawlerSession.SQLPass);
			System.out.println("Connection: "+connection);
		}
		catch(Exception sqle)
		{
			System.err.println(EsaphServerCrawlerSession.placeholder + "Exception(createNewConnectionsForPool()): "+ sqle);
			return null;
		}

		return connection;
	}
	
	@Override
	public void run()
	{
		try
		{
			System.out.println(EsaphServerCrawlerSession.placeholder + "Crawler running...");
			this.connection = this.createNewConnectionForPool();
			if(this.connection != null)
			{
				System.out.println(EsaphServerCrawlerSession.placeholder + "Crawler Connected to SQL-Server.");
				int currentPosition = 0;
				while(currentPosition > -1)
				{
					PreparedStatement prGetPrivatePostsPassed24Hours
					= (PreparedStatement) this.connection.prepareStatement(EsaphServerCrawlerSession.queryGetPrivatePostsPassed24Hours,
							  ResultSet.TYPE_SCROLL_INSENSITIVE,
							  ResultSet.CONCUR_READ_ONLY);
					prGetPrivatePostsPassed24Hours.setInt(1, currentPosition);
					
					ResultSet result = prGetPrivatePostsPassed24Hours.executeQuery();
					if(result.next())
					{
						try
						{
							result.last();
							currentPosition = currentPosition + result.getRow();
						    result.beforeFirst();
						    result.next();
						}
						catch(Exception ex)
						{
							System.out.println(EsaphServerCrawlerSession.placeholder + "Failed get Row Count Private Groups Posts: " + ex);
						}
						
						do
						{
							PreparedStatement preparedStatementDeleteHashtags = (PreparedStatement)
							this.connection.prepareStatement(EsaphServerCrawlerSession.queryRemoveHashtagsFromPost);
							preparedStatementDeleteHashtags.setLong(1, result.getLong("PPID"));
							preparedStatementDeleteHashtags.executeUpdate();
							preparedStatementDeleteHashtags.close();

							File fileHQ = getStoringFile(result.getString("PID"));
							if(fileHQ != null)
							{
								fileHQ.delete();
							}

							PreparedStatement prDelete = (PreparedStatement)
									this.connection.prepareStatement(EsaphServerCrawlerSession.queryRemovePostingPrivateMoments);
							prDelete.setLong(1, result.getLong("PPID"));
							prDelete.executeUpdate();
							prDelete.close();
						}
						while(result.next());
					}
					else
					{
						currentPosition = -2;
						break;
					}

					prGetPrivatePostsPassed24Hours.close();
					result.close();
				}

				System.out.println(EsaphServerCrawlerSession.placeholder + "Crawler Finished Private Postings.");
			}
		}
		catch(Exception ec)
		{
			System.out.println(EsaphServerCrawlerSession.placeholder + "fatal Error: " + ec);
		}
		finally
		{
			try
			{
				System.out.println(EsaphServerCrawlerSession.placeholder + "Finishing Crawler-Job.");
				if(this.connection != null && !this.connection.isClosed())
				{
					this.connection.close();
					System.out.println(EsaphServerCrawlerSession.placeholder + "Finishing Crawler-Job: Connection closed");
				}
				System.out.println(EsaphServerCrawlerSession.placeholder + "Finishing Crawler-Job: Finished");
			}
			catch (SQLException e)
			{
				System.out.println(EsaphServerCrawlerSession.placeholder + "Finishing Crawler-Job FAILED: " + e);
			}
		}
	}


    private File getStoringFile(String PID)
    {
        StringBuilder builderCache = new StringBuilder();
        builderCache.append(EsaphStoragePaths.PATH_PRIVATE_UPLOADS);
        builderCache.append(File.separator);
        builderCache.append(getFolderFilePath(PID));
        return new File(builderCache.toString());
    }

    private String getFolderFilePath(String mainDirectory) //Überprüft auch automatisch ob das datei Limit erreicht wurde, wenn ja dann wird ein neuer ordner angelegt.
    {
        StringBuilder stringBuilder = new StringBuilder();
        for(int counter = 0; counter < mainDirectory.length(); counter++)
        {
            stringBuilder.append(mainDirectory.substring(counter, counter+1));
            stringBuilder.append(File.separator);
        }
        stringBuilder.append(mainDirectory);
        return stringBuilder.toString();
    }
}
