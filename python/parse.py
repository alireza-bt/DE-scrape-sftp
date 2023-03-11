import pandas as pd
from bs4 import BeautifulSoup
from pip import main
import sys

def get_html_value(html_item, first_tag, class_or_id, second_tag=None, value_attr='text'):
    html = html_item.find(first_tag, class_=class_or_id)
    if html is not None:
        if second_tag is None:
            return getattr(html, value_attr)
        else:
            return getattr(getattr(html, second_tag), value_attr)
        #print("title: %s"%job_title.a.text)
    else:
        return None


if __name__ == '__main__':

    if len(sys.argv) < 3 or len(sys.argv[1]) < 6 or len(sys.argv[2]) < 5:
        sys.exit("Arguments are not valid")

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    with open("./%s"%input_file) as fp:
        soup = BeautifulSoup(fp, "html.parser")

    #create a dataframe and store these
    result_df = pd.DataFrame(columns=['job_title', 'company', 'location', 'rating', 'contract', 'fast_apply', 'publish_date'])

    try:
        list_of_items = soup.find("ul", class_="jobsearch-ResultsList")
        
        for item in list_of_items.find_all("li"):
            item_dict = {}
            df_col_names = result_df.columns
            
            main_information = item.find("td", class_="resultContent")
            
            if main_information is None:
                continue
        
            #details = item.find_all("td", class_="resultContent")
            item_dict[df_col_names[0]] = get_html_value(main_information, "h2", "jobTitle", "a")
            """ job_title = item.find("h2", class_="jobTitle")
            if job_title is not None:
                item_dict[df_col_names[0]] = job_title.a.text
                #print("title: %s"%job_title.a.text)
            else:
                item_dict[df_col_names[0]] = None """

            item_dict[df_col_names[1]] = get_html_value(main_information, "span", "companyName")
            """company = item.find("span", class_="companyName")
            if company is not None:
                item_dict[df_col_names[1]] = company.text
                #print("company: %s"%company.text)
            else:
                item_dict[df_col_names[1]] = None"""

            item_dict[df_col_names[2]] = get_html_value(main_information, "div", "companyLocation")
            """ location = item.find("div", class_="companyLocation")
            if location is not None:
                item_dict[df_col_names[2]] = location.text
                #print("location: %s"%location.text)
            else:
                item_dict[df_col_names[2]] = None """

            item_dict[df_col_names[3]] = get_html_value(main_information, "span", "ratingNumber", "span")
            """ company_rating = item.find("span", class_="ratingNumber")
            if company_rating is not None:
                item_dict[df_col_names[3]] = company_rating.span.text
                #print("company rating: %s"%company_rating.span.text)
            else:
                item_dict[df_col_names[3]] = None """
            
            metadata = item.find("div", class_="metadata")
            item_dict[df_col_names[4]] = None
            if metadata is not None:
                item_dict[df_col_names[4]] = get_html_value(metadata, "div", "attribute_snippet")
                """ contract_type = metadata.find("div", class_="attribute_snippet")
                if contract_type is not None:
                    item_dict[df_col_names[4]] = contract_type.text """
                    #print("contract type: %s"%contract_type.text)

            fast_apply = item.find("td", class_="indeedApply")
            item_dict[df_col_names[5]] = False
            if fast_apply is not None:
                item_dict[df_col_names[5]] = True
                #print("schnellbewerbung")
            
            item_dict[df_col_names[6]] = get_html_value(item, "span", "date").replace('Posted', '').replace('geschaltet', '')
            """ date_published = item.find("span", class_="date")
            if date_published is not None:
                date_text=date_published.text.replace('Posted', '').replace('geschaltet', '')
                item_dict[df_col_names[6]] = date_text
                #print("published date: %s"%date_text)
            else:
                item_dict[df_col_names[6]] = None """
            
            result_df = result_df.append(item_dict, ignore_index=True)
            #print('')
            
            #break
    except Exception as e:
        print(e)
    
    #result_df.to_csv(, index=False, sep=';', mode='a')
 
    with open(output_file, 'a') as f:
       result_df.to_csv(f, index=False, sep=';', mode='a', header=f.tell()==0)