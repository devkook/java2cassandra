package devkook.study.java2cassandra;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Hello CASSANDRA !
 * 
 * bin/cassandra-cli connect localhost/9160 CREATE KEYSPACE hectortestkeyspace
 * with placement_strategy = ‘SimpleStrategy’ and strategy_options =
 * {replication_factor:1}; USE hectortestkeyspace; CREATE COLUMN FAMILY
 * hectortestcolumfamily;
 */

public class App {
	String clusterName = "FBWOTJQ";
	String hostInfomation = "127.0.0.1:9160";
	String keyspaceName = "hectortestkeyspace";
	String columnfamilyName = "hectortestcolumfamily";
	String columnName = "fake_column_1";
	String rowkey = "hectortestkey1";

	void runrunCassandra() {
		// 카산드라 클러스터를 선택한다. (클러스터명이 맞지 않아도 작동에 문제가 없다..으잉???)
		Cluster cluster = HFactory.getOrCreateCluster(clusterName,
				hostInfomation);
		// 해당 키스페이스를 선택한다.
		Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
		// 아마 데이터 변환관련일을 하는것으로 추측된다..
		StringSerializer stringSerializer = StringSerializer.get();
		// 값을 삽입한다.
		// ‘hectortestcolumfamily’ 라는 컬럼패밀리에 ‘hectortestkey1’라는 키를 가진 열에
		// ‘fake_column_1’라는 컬럼이름을 가진 곳에 ‘fake_value_1’라는 값을 넣어준다.
		Mutator<String> mutator = HFactory.createMutator(keyspace,
				stringSerializer);
		mutator.insert(rowkey, columnfamilyName,
				HFactory.createStringColumn(columnName, "fake_value_1"));
		// 값을 읽어온다.
		// 값을 읽어오기 위한 쿼리오브젝트관련 셋팅한다.
		ColumnQuery<String, String, String> columnQuery = HFactory
				.createColumnQuery(keyspace, stringSerializer,
						stringSerializer, stringSerializer);
		columnQuery.setColumnFamily(columnfamilyName);
		columnQuery.setName(columnName);
		columnQuery.setKey(rowkey);
		// 결과에서 값을 읽어온다.
		QueryResult<HColumn<String, String>> queryResult = columnQuery
				.execute();
		HColumn<String, String> hColumn = queryResult.get();
		String result = hColumn.getValue();
		System.out.println(result);
		// 현재 넣은 데이터를 삭제한다.
		mutator.delete(rowkey, columnfamilyName, columnName, stringSerializer);
		// 삭제된 값을 확인하기 위해 값을 읽어온다.
		// 값을 읽어오기 위한 쿼리오브젝트관련 셋팅한다.
		columnQuery = HFactory.createColumnQuery(keyspace, stringSerializer,
				stringSerializer, stringSerializer);
		columnQuery.setColumnFamily(columnfamilyName);
		columnQuery.setName(columnName);
		columnQuery.setKey(rowkey);
		// 결과에서 값을 읽어온다. 데이터가 없으므로 "nullÏ이라는 갑이 나온다.
		queryResult = columnQuery.execute();
		hColumn = queryResult.get();
		System.out.println(hColumn);
	}
}
