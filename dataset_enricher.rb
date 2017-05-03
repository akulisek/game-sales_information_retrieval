require 'HTTParty'
require 'csv'
require 'json'

class DatasetEnricher

  GAMES_DATASET = 'dataset/Video_Game_Sales_as_of_Jan_2017.csv'
  OUTPUT_CSV_PATH = 'dataset/Predecessors_Dataset_'+Time.now.strftime('%Y-%m-%d_%H-%M-%S')+'.csv'

  ELASTICSEARCH_IP = 'http://127.0.0.1:9200/'

  ELASTICSEARCH_INDEX_GAME = 'games/game/'

  def self.save_data
    read_and_parse(GAMES_DATASET, ELASTICSEARCH_INDEX_GAME)
  end

  private

  def self.read_and_parse(path, index)
    games = CSV.read(path,
                      headers: :first_row,
                      encoding: 'utf-8',
                      :row_sep => :auto,
                      :col_sep => ','
    )
    parsed_games = parse_csv_games(games)

    predecessors = {}

    parsed_games.each do |game|
      begin
        response_json = find_predecessors(game, index)
      rescue StandardError=>e
        puts "\tError: #{e}"
      else
        predecessors[game[:id]] = response_json
        puts "\t Success: #{game[:id]}"
      end
    end
    predecessors_data = get_predecessors_data(parsed_games, predecessors)
    save_as_csv(parsed_games, predecessors_data)
  end

  def self.parse_csv_games(games)
    parsed_games = []
    if games
      games.each_with_index do |row, index|
        game = {}
        game[:id] = index.to_i
        game[:name] = row['Name']
	      colon_index = row['Name'].index(':')
	      series_name = row['Name'][0..(colon_index-1)] if colon_index
	      game[:series] = series_name || ""
        game[:platform] = row['Platform']
        game[:year_of_release] = row['Year_of_Release'].to_i
        game[:genre] = row['Genre']
        game[:publisher] = row['Publisher']
        game[:na_sales] = row['NA_Sales'].to_f
        game[:eu_sales] = row['EU_Sales'].to_f
        game[:jp_sales] = row['JP_Sales'].to_f
        game[:othes_sales] = row['Other_Sales'].to_f
        game[:global_sales] = row['Global_Sales'].to_f
        game[:critic_score] = row['Critic_Score'].to_f
        game[:critic_count] = row['Critic_Count'].to_i
        game[:user_score] = row['User_Score'].to_f
        game[:user_count] = row['User_Count'].to_i
        game[:rating] = row['Rating']
        parsed_games << game
      end
    end
    parsed_games
  end

  def self.find_predecessors(game_hash, index)
  # by default set up for a game with series
  body = { 'query' => {
            'bool' => {
               'must' => [
                    {
                        'more_like_this' => {
                               'fields' => [
                                  'name'
                               ],
                               'like': [
                                   {
                                        '_id' => game_hash[:id],
                                        '_index' => 'games',
                                        '_type' => 'game'
                                   }
                               ],
                               'min_doc_freq' => 1,
                               'boost_terms' => 100,
                               'min_term_freq' => 1,
                               'max_query_terms' => 100,
                               'minimum_should_match' => '50%'
                        }
                    },
                    {
                        'range' => {
                            'global_sales' => {
                                'gte' => 0
                            }
                        }
                    },
                    # used only if series is present
                    {
                        'more_like_this': {
                               'fields': [
                                  'series.raw'
                               ],
                               'like': [
                                   {
                                        '_id': game_hash[:id],
                                        '_index': 'games',
                                        '_type': 'game'
                                   }
                               ],
                               'min_doc_freq': 1,
                               'boost_terms': 100,
                               'min_term_freq': 1,
                               'max_query_terms': 100,
                               'minimum_should_match': '100%'
                        }
                    }
               ]
            }
          }
    }
    # find predecessors based on name only, 100% of terms must match
    if game_hash[:series] == ""
      body['query']['bool']['must'].pop
      body['query']['bool']['must'][0]['more_like_this']['minimum_should_match'] = '100%'
    end

    puts "GET #{ELASTICSEARCH_IP + index}_search?&size=1000"
    response = HTTParty.post(ELASTICSEARCH_IP + index.to_s + "_search?&size=1000",
                  :body => body.to_json,
                  :headers => { 'Content-Type' => 'application/json' }
    )
    json = JSON.parse(response.body)
  end

  def self.get_predecessors_data(parsed_games, predecessors)
    result = {}
    predecessors.each do |key, value|
      mean, sum, count = 0, 0, 0
      # iterate over found similar games
      value['hits']['hits'].each do |hit|
        if parsed_games[key.to_i][:genre] == hit['_source']['genre']
          sum += hit['_source']['global_sales'].to_f
          count += 1
        end
      end
      print "sum #{sum} count #{count}"
      mean = sum / count if count > 0
      result[key] = { :count => value['hits']['total'], :sales_mean => mean }
    end
    result
  end

  def self.save_as_csv(games, predecessors)
    columns = %w(Name Platform Year_of_Release Genre Publisher NA_Sales
      EU_Sales JP_Sales Other_Sales Global_Sales Critic_Score Critic_Count
      User_Score User_Count Rating Predecessors_Count Predecessors_Sales_Mean)

    CSV.open(OUTPUT_CSV_PATH, 'w', {headers: true, col_sep: ';'}) do |csv|
      csv << columns
      games.each do |game|
        # filter out index
        row = game.values[1..-1] + predecessors[game[:id].to_i].values
        csv << row
      end
    end
  end

end

DatasetEnricher.save_data()
