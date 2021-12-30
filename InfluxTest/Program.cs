using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using InfluxData.Net.Common.Enums;
using InfluxData.Net.InfluxDb;
using InfluxData.Net.InfluxDb.Models;
using StackExchange.Redis;
using Stopwatch = InfluxData.Net.Common.Helpers.Stopwatch;

namespace InfluxTest
{
    class Program
    {
        static string nameOfDB = "TestDB";

        static int countSet = 500; //Сколько раз отправаляем данные
        static bool onlyOnePoint = true;
        private static int countOfPoints = 20;

        static int countArrays = 0; //количество больших объектов
        static int countFloats = 10000/ countOfPoints; //количество float'ов
        static int sizeDataArray = 1024 * 2; //размер объекта в срезе (x 1.3 так как Base64)

        static int offsetInt = 0;
        static int offsetForTime = 0;
        static float offsetForFloat = 0f;


        static DateTime dt = new DateTime(1970, 1, 1, 0, 0, 0, 0);
        static void Main(string[] args)
        {
            var localInfluxIp = "127.0.0.1";
            var localInfluxClient = new InfluxDbClient($"http://{localInfluxIp}:8086", "", "", InfluxDbVersion.Latest);
            var dbCreating = Task.Run(async () => await localInfluxClient.Database.CreateDatabaseAsync(nameOfDB));
            dbCreating.Wait();

            for (int i = 0; i < 5; i++)
            {
                SetInInflux(localInfluxClient);
                Console.WriteLine("-- "+i);
            }

            Stopwatch stopwatchTatget = Stopwatch.StartNew();
            SetInInflux(localInfluxClient);
            stopwatchTatget.Stop();
            Console.WriteLine($"Time: {stopwatchTatget.Elapsed.TotalMilliseconds}");

            GetDataFromInflux(localInfluxClient);


			Console.ReadKey();
        }

        private static void GetDataFromInflux(IInfluxDbClient influxDbClient)
        {

			var keys = influxDbClient.Serie.GetMeasurementsAsync( nameOfDB ).Result.Select( measurement => measurement.Name );

			var request = $"SELECT * FROM \"{string.Join( "\",\"", keys )}\" where time <= {(dt + TimeSpan.FromSeconds(10)).Ticks}";
	        var response = influxDbClient.Client.QueryAsync( request, nameOfDB ).Result;

	        var pairsToSend = response.Select( serie =>
		        new KeyValuePair<RedisKey, RedisValue>( serie.Name, serie.Values[0][1].ToString() ) ).ToArray();

	        int awd = 0;
			return;
	        for ( int i = 0; i < pairsToSend.Length; i++ )
	        {
		        if ( pairsToSend[i].Key.ToString().StartsWith( "[Bytes]" ) )
		        {
			        pairsToSend[i] = new KeyValuePair<RedisKey, RedisValue>( pairsToSend[i].Key, Convert.FromBase64String( pairsToSend[i].Value ) );
		        }
	        }
        }

        private static List<Point> GetListOfPoints()
        {
            var pointsToSend = new List<Point>();
            if (onlyOnePoint)
            {
	            for (int i = 0; i < countOfPoints; i++)
	            {
		            var point = new Point
		            {
			            Name = $"Name{i}",
			            Fields = new Dictionary<string, object>()
		            };

		            for ( int k = 0; k < countArrays; k++ )
		            {
			            point.Fields.Add( $"valueArray{k}", Convert.ToBase64String( CreateByteArray( sizeDataArray ) ) );
		            }
		            for ( int j = 0; j < countFloats; j++ )
		            {
			            offsetForFloat += 0.00001f;
			            point.Fields.Add( $"valueFloat{j}", offsetForFloat );
		            }
		            point.Timestamp = dt + TimeSpan.FromSeconds( offsetForTime );
		            pointsToSend.Add( point );
                }
			}
            else
            {
	            for ( var i = 0; i < countArrays; i++ )
	            {
		            var point = new Point
		            {
			            Name = $"Name{i}",
		            };
		            point.Fields[$"valueArray"] = Convert.ToBase64String( CreateByteArray( sizeDataArray ) );
		            
		            point.Timestamp = dt + TimeSpan.FromSeconds( offsetForTime );
		            pointsToSend.Add( point );
	            }
	            for ( var i = 0; i < countFloats; i++ )
	            {
		            var point = new Point
		            {
			            Name = $"Name{i}",
		            };
		            offsetForFloat += 0.00001f;
                    point.Fields[$"valueFloat"] = offsetForFloat;
		            
		            point.Timestamp = dt + TimeSpan.FromSeconds( offsetForTime );
		            pointsToSend.Add( point );
	            }

            }

            offsetForTime++;
            return pointsToSend;
        }

        private static byte[] CreateByteArray(int length)
        {
            var array = new byte[length];
            for (int i = 0; i < length; i++)
            {
                array[i] = (byte)(i+ offsetInt);
            }

            offsetInt++;
            return array;
        }

        private static void SetInInflux(InfluxDbClient client )
        {
            for (int i = 0; i < countSet; i++)
            {
                List<Point> pointsToSend = GetListOfPoints();
                if(pointsToSend.Count == 1)
					Task.WaitAll(client.Client.WriteAsync(pointsToSend.First(), nameOfDB));
                else
	                Task.WaitAll( client.Client.WriteAsync( pointsToSend, nameOfDB ) );
            }
        }
    }
}
