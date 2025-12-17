# %%
import pandas as pd

# %%
class SearchData:
    def __init__(self, df, keyword_str, ai_titles=None, ai_summaries=None, title=None, typenames=None, uploader=None, pubdate_inf=None, pubdate_sup=None, tags_list=None, duration_inf=None, duration_sup=None, lan= None, search_mode=0, limit=10, offset=0):
        self.df = df
        self.df['scenes_str'] = self.df['scenes'].apply(str)
        self.df['subtitles_str'] = self.df['subtitles'].apply(str)
        self.keyword_str = keyword_str
        if len(keyword_str) == 0:
            raise ValueError("Keyword string cannot be empty.")
        # check whether the keyword_str is a string of spaces(including tabs and new lines)
        if len(keyword_str.strip()) == 0:
            raise ValueError("Keyword string cannot be empty.")
        self.ai_titles = ai_titles
        self.ai_summaries = ai_summaries
        self.title = title
        self.typenames = typenames # a list of strings
        self.uploader = uploader
        self.pubdate_inf = pubdate_inf
        self.pubdate_sup = pubdate_sup
        self.tags_list = tags_list
        self.duration_inf = duration_inf
        self.duration_sup = duration_sup
        self.lan = lan
        self.offset = offset
        self.results = None
        self.searched = False
        self.search_mode = search_mode
        self.limit = limit
        self.results_count = 0
        # search_mode = 0 means search in subtitles and scenes
        # search_mode = 1 means search in subtitles only
        # search_mode = 2 means search in scenes only

    @staticmethod
    def sliding_window(list0, window_size=3):
        list_temp = []
        if len(list0) == 0:
            return list_temp
        if len(list0) < window_size:
            list_temp.append(list0)
        else:
            for i in range(len(list0) - window_size + 1):
                list_temp.append(list0[i:i + window_size])
        return list_temp
    
    @staticmethod
    def subtitle_filter(list_original,keyword):
        try:
            df_temp = pd.DataFrame(list_original)
            
            # You must write this function as a closure!!!!!!!
            def scene_filte_0(dict0):
                # print(type(dict0))
                return (keyword in dict0.get('c'))
            mask_temp = df_temp[1].apply(scene_filte_0)
            list_temp = []
            # # append every cell value to the list_temp
            if len(df_temp[mask_temp]) >= 1:
                for j in range(len(df_temp[mask_temp].iloc[0])):
                    list_temp.append(df_temp[mask_temp].iloc[0][j])
                return list_temp
            else:
                return []
        except KeyError:
            # print("KeyError: 'c' not found in the dictionary")
            return []
    
    @staticmethod
    def scene_filter(list_original, keyword):
        try:
            # list_original = df_raw_2['scenes_windows'].iloc[0]
            df_temp = pd.DataFrame(list_original)
            
            # You must write this function as a closure!!!!!!!
            def scene_check(cell_dict):
                cell_temp = pd.DataFrame(list(cell_dict.values())[0])
                # check whether the keyword is in the any row of the column `c`
                return cell_temp['c'].str.contains(keyword).any()
            
            # apply the function on column 1
            mask_temp = df_temp[1].apply(scene_check)
            list_temp = []
            # # append every cell value to the list_temp
            if len(df_temp[mask_temp]) >= 1:
                for j in range(len(df_temp[mask_temp].iloc[0])):
                    list_temp.append(df_temp[mask_temp].iloc[0][j])
                return list_temp
            else:
                return []
        except KeyError:
            # print("KeyError: 'c' not found in the dictionary")
            return []
    
    # return rows where column `scenes` or `subtitle` contains the keyword
    def first_filter(self, df, keyword_str, ai_titles=None, ai_summaries=None, title=None, typenames=None, uploader=None, pubdate_inf=None, pubdate_sup=None, tags_list=None, duration_inf=None, duration_sup=None, lan= None,search_mode=0,limit=10, offset=0):
        filtered_df = df.copy()
        # filter by ai_titles:
        if ai_titles is not None:
            filtered_df = filtered_df[filtered_df['ai_titles'].str.contains(ai_titles, case=False, na=False)]
        
        # filter by ai_summaries:
        if ai_summaries is not None:
            filtered_df = filtered_df[filtered_df['ai_summaries'].str.contains(ai_summaries, case=False, na=False)]

        # filter by title:
        if title is not None:
            filtered_df = filtered_df[filtered_df['title'].str.contains(title, case=False, na=False)]
        
        # filter by typenames:
        # typenames is a list of strings
        # return rows if the field `typenames` matches one element in the list `typenames`
        # i.e. an OR operation
        if typenames is not None:
            # convert the list of strings to a regex pattern
            typenames_pattern = '|'.join(typenames)
            filtered_df = filtered_df[filtered_df['typenames'].str.contains(typenames_pattern, case=False, na=False)]
        
        # filter by uploader:
        if uploader is not None:
            filtered_df = filtered_df[filtered_df['uploader'].str.contains(uploader, case=False, na=False)]

        # filter by pubdate_inf: 
        # note that pubdate_inf is type int, while pubdate is type `numpy.int64`
        if pubdate_inf is not None:
            filtered_df = filtered_df[filtered_df['pubdate'] >= pubdate_inf]
        
        # filter by pubdate_sup:
        if pubdate_sup is not None:
            filtered_df = filtered_df[filtered_df['pubdate'] <= pubdate_sup]
        
        # filter by tags_list:
        if tags_list is not None:
            # the column 'tags' is a list of strings
            # the tags_list is also a list of strings
            # Filter rows where 'tags' contains all of the tags in tags_list
            # i.e. an AND operation
            filtered_df = filtered_df[filtered_df['tags'].apply(lambda x: all(tag in x for tag in tags_list))]

        # filter by duration_inf:
        if duration_inf is not None:
            filtered_df = filtered_df[filtered_df['duration'] >= duration_inf]
        
        # filter by duration_sup:
        if duration_sup is not None:
            filtered_df = filtered_df[filtered_df['duration'] <= duration_sup]
        
        # filter by lan:
        if lan is not None:
            filtered_df = filtered_df[filtered_df['lan'] == lan]   

        # filter by search_mode:
        if search_mode == 1:
            # filter by keyword in subtitles only
            # make the column `scenes` to be all `[]`
            filtered_df['scenes'] = [[] for _ in range(len(filtered_df))]
        elif search_mode == 2:
            # filter by keyword in scenes only
            # make the column `subtitles` to be all `[]`
            filtered_df['subtitles'] = [[] for _ in range(len(filtered_df))]

        # region First filter by keyword:
        keyword = keyword_str

        # Filter rows where 'scenes' or 'subtitle' contains the keyword
        filtered_df = filtered_df[(filtered_df['scenes_str'].str.contains(keyword, case=False, na=False) | filtered_df['subtitles_str'].str.contains(keyword, case=False, na=False))]

        # create two new columns `scenes_flag` and `subtitles_flag` to indicate whether the keyword is in the column `scenes` or `subtitle`
        filtered_df['scenes_flag'] = filtered_df['scenes_str'].str.contains(keyword, case=False, na=False)
        filtered_df['subtitles_flag'] = filtered_df['subtitles_str'].str.contains(keyword, case=False, na=False)

        # if the row has a False value for column 'scenes_flag', make the column `scenes` to be `[]` in the same row
        filtered_df.loc[filtered_df['scenes_flag'] == False, 'scenes'] = filtered_df.loc[filtered_df['scenes_flag'] == False, 'scenes'].apply(lambda x: [])
        # if the row has a False value for column 'subtitles_flag', make the column `subtitles` to be `[]` in the same row
        filtered_df.loc[filtered_df['subtitles_flag'] == False, 'subtitles'] = filtered_df.loc[filtered_df['subtitles_flag'] == False, 'subtitles'].apply(lambda x: [])

        # filter out rows where subtitles_windows and scenes_windows are both empty lists
        filtered_df = filtered_df[filtered_df['subtitles'].map(lambda x: len(x) > 0) | filtered_df['scenes'].map(lambda x: len(x) > 0)]

        # give the count
        self.results_count = len(filtered_df)

        # apply `LIMIT` here to speed up the process!!!
        filtered_df = filtered_df.iloc[offset:offset+limit]
        # endregion
        
        # region Second filter by keyword:
        # create windows for subtitles and scenes
        filtered_df['subtitles_windows'] = filtered_df['subtitles'].apply(lambda x: SearchData.sliding_window(x, 3))
        filtered_df['scenes_windows'] = filtered_df['scenes'].apply(lambda x: SearchData.sliding_window(x, 3))
        filtered_df_2 = filtered_df[['bvids', 'ai_titles', 'ai_summaries', 'title',
            'typenames', 'uploader', 'pubdate', 'tags', 'rank_scores', 'upics',
            'lan', 'subtitles_windows',
            'scenes_windows']]
        
        # filter by keyword:
        filtered_df_2['subtitles_windows'] = filtered_df_2['subtitles_windows'].apply(lambda x: SearchData.subtitle_filter(x, keyword))
        filtered_df_2['scenes_windows'] = filtered_df_2['scenes_windows'].apply(lambda x: SearchData.scene_filter(x, keyword))

        # filter out rows where subtitles_windows and scenes_windows are both empty lists
        filtered_df_2 = filtered_df_2[filtered_df_2['subtitles_windows'].map(lambda x: len(x) > 0) | filtered_df_2['scenes_windows'].map(lambda x: len(x) > 0)]
        # endregion

        # region order by:
        # order by rank_scores
        filtered_df_2 = filtered_df_2.sort_values(by=['rank_scores'], ascending=False)
        # reset the index
        filtered_df_2 = filtered_df_2.reset_index(drop=True)
        # endregion
        
        return filtered_df_2
    
    def get_results(self):
        self.results = self.first_filter(self.df, self.keyword_str, self.ai_titles, self.ai_summaries, self.title, self.typenames, self.uploader, self.pubdate_inf, self.pubdate_sup, self.tags_list, self.duration_inf, self.duration_sup, self.lan, self.search_mode, self.limit, self.offset)
        self.searched = True
        return self.results
    
    def get_json_results(self):
        # check whether self.results is an empty list
        if self.searched:
            # check whether self.results is an empty DataFrame
            if not self.results.empty:
                results_count = self.results_count
                # convert the results to json format
                results = self.results.to_dict(orient='records')
                json_results = dict()
                json_results['results_count'] = results_count
                json_results['results'] = results
                return json_results
            else:
                print("No results found.")
                return []
        else:
            print("You must call get_results() first.")