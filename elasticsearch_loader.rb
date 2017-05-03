require 'HTTParty'
require 'csv'
require 'json'

class ElasticsearchGamesLoader

  GAMES_DATASET = 'dataset/Video_Game_Sales_as_of_Jan_2017.csv'

  ELASTICSEARCH_IP = 'http://127.0.0.1:9200'

  ELASTICSEARCH_INDEX_GAME = '/games/game/'


  def self.load_data
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

    parsed_games.each do |game|
      begin
        response = post_game(game, index)
      rescue StandardError=>e
        puts "\tError: #{e}"
      else
        puts "\t Success: #{response}"
      end
    end
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
        game[:other_sales] = row['Other_Sales'].to_f
        game[:global_sales] = row['Global_Sales'].to_f
        game[:critic_score] = row['Critic_Score'].to_f
        game[:critic_count] = row['Critic_Count'].to_i
        game[:user_score] = row['User_Score'].to_f
        game[:user_count] = row['User_Count'].to_i
        parsed_games << game
      end
    end
    parsed_games
  end

  def self.post_game(game_hash, index)
    puts "Posting game to #{ELASTICSEARCH_IP + index + game_hash[:id].to_s}"
    HTTParty.post(ELASTICSEARCH_IP + index.to_s + game_hash[:id].to_s,
                  :body => game_hash.to_json,
                  :headers => { 'Content-Type' => 'application/json' }
    )
  end

  # OUTDATED INDEX
  def self.put_index
    puts "PUT index #{ELASTICSEARCH_IP}"
    HTTParty.put(ELASTICSEARCH_IP + '/games',
                    :body => {
                      "settings" => {
                          "analysis" => {
                              "filter" => {
                                  "games_quantity_stop" => {
                                      "type" => "stop",
                                      "stopwords" => [ "one", "two"]
                                  },
                                  "english_stop"=> {
                                      "type"=>       "stop",
                                      "stopwords"=>  "_english_"
                                  },
                                  "english_stemmer"=> {
                                      "type"=>       "stemmer",
                                      "language"=>   "english"
                                  },
                                  "english_possessive_stemmer"=> {
                                      "type"=>       "stemmer",
                                      "language"=>   "possessive_english"
                                  },
                                  "my_char_filter"=> {
                                      "type"=> "pattern_replace",
                                      "pattern"=> "(\\d+)",
                                      "replacement"=> ""
                                  }
                              },
                              "char_filter"=> {
                                  "quotes_mapping"=> {
                                      "type"=> "mapping",
                                      "mappings"=> [
                                        "\\u0091=>\\u0027",
                                        "\\u0092=>\\u0027",
                                        "\\u2018=>\\u0027",
                                        "\\u2019=>\\u0027",
                                        "\\u201B=>\\u0027"
                                      ]
                                  }
                              },
                              "analyzer"=> {
                                  "games_english"=> {
                                      "tokenizer"=>  "standard",
                                      "char_filter"=> [ "quotes_mapping" ],
                                      "filter"=> [
                                        "my_char_filter",
                                        "lowercase",
                                        "english_possessive_stemmer",
                                        "english_stemmer",
                                        "english_stop",
                                        "games_quantity_stop"
                                      ]
                                  }
                              }
                          }
                      },
                              "mappings" => {
                              "recipe" => {
                              "properties" => {
                              "name"=> {
                                      "type"=> "string",
                                      "index"=> "analyzed",
                                      "analyzer"=> "english"
                                  },
                              "Platform"=> {
                                      "type"=> "string",
                                      "index"=> "not_analyzed"
                              },
                              "Year_of_Release"=> {
                                      "type"=> "integer"
                              },
                              "Genre"=> {
                                      "type"=> "string",
                                      "index"=> "not_analyzed"
                              },
                              "Publisher"=> {
                                      "type"=> "string",
                                      "index"=> "not_analyzed"
                              },
                              "NA_Sales"=> {
                                      "type"=> "integer"
                              },
                              "EU_Sales"=> {
                                      "type"=> "integer"
                              },
                              "JP_Sales"=> {
                                      "type"=> "integer"
                              },
                              "Global_Sales"=> {
                                      "type"=> "integer"
                              },
                              "Other_Sales"=> {
                                      "type"=> "integer"
                              },
                              "Critic_Score"=> {
                                      "type"=> "integer"
                              },
                              "Critic_Count"=> {
                                      "type"=> "integer"
                              },
                              "User_Score"=> {
                                      "type"=> "integer"
                              },
                              "User_Count"=> {
                                      "type"=> "integer"
                              }
                            }
                          }
                        }
                      }.to_json,
                    :headers => { 'Content-Type' => 'application/json' }
      )
  end

end
if ARGV.count == 1 && ARGV[0].class == String && (ARGV[0] == '--help' || ARGV[0] == '-h')
  puts "USAGE: \n"
  puts "\t - $ ruby elasticsearch_loader.rb"
  puts "\t - <document_type> is either \'deal\' or \dealitem\'"
else
  ElasticsearchGamesLoader.load_data()
end
