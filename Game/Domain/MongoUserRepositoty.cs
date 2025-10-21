using System;
using MongoDB.Driver;

namespace Game.Domain
{
    public class MongoUserRepository : IUserRepository
    {
        private readonly IMongoCollection<UserEntity> userCollection;
        public const string CollectionName = "users";

        public MongoUserRepository(IMongoDatabase database)
        {
            userCollection = database.GetCollection<UserEntity>(CollectionName);
            
            var indexKeysDefinition = Builders<UserEntity>.IndexKeys.Ascending(u => u.Login);
            var indexOptions = new CreateIndexOptions { Unique = true };
            var indexModel = new CreateIndexModel<UserEntity>(indexKeysDefinition, indexOptions);
            userCollection.Indexes.CreateOne(indexModel);
        }

        public UserEntity Insert(UserEntity user)
        {
            var userToInsert = new UserEntity(
                Guid.NewGuid(),
                user.Login,
                user.LastName,
                user.FirstName,
                user.GamesPlayed,
                user.CurrentGameId);
            userCollection.InsertOne(userToInsert);
            return userToInsert;
        }

        public UserEntity FindById(Guid id)
        {
            return userCollection.Find(u => u.Id == id).FirstOrDefault();
        }

        public UserEntity GetOrCreateByLogin(string login)
        {
            var filter = Builders<UserEntity>.Filter.Eq(u => u.Login, login);
            var update = Builders<UserEntity>.Update
                .SetOnInsert(u => u.Id, Guid.NewGuid())
                .SetOnInsert(u => u.Login, login)
                .SetOnInsert(u => u.FirstName, "")
                .SetOnInsert(u => u.LastName, "")
                .SetOnInsert(u => u.GamesPlayed, 0)
                .SetOnInsert(u => u.CurrentGameId, null);
            
            var options = new FindOneAndUpdateOptions<UserEntity>
            {
                IsUpsert = true,
                ReturnDocument = ReturnDocument.After
            };
            
            try
            {
                return userCollection.FindOneAndUpdate(filter, update, options);
            }
            catch (MongoCommandException ex) when (ex.Code == 11000)
            {
                return userCollection.Find(filter).FirstOrDefault();
            }
        }

        public void Update(UserEntity user)
        {
            userCollection.ReplaceOne(u => u.Id == user.Id, user);
        }

        public void Delete(Guid id)
        {
            userCollection.DeleteOne(u => u.Id == id);
        }

        // Для вывода списка всех пользователей (упорядоченных по логину)
        // страницы нумеруются с единицы
        public PageList<UserEntity> GetPage(int pageNumber, int pageSize)
        {
            var totalCount = userCollection.CountDocuments(FilterDefinition<UserEntity>.Empty);
            var items = userCollection
                .Find(FilterDefinition<UserEntity>.Empty)
                .SortBy(u => u.Login)
                .Skip((pageNumber - 1) * pageSize)
                .Limit(pageSize)
                .ToList();
            return new PageList<UserEntity>(items, totalCount, pageNumber, pageSize);
        }

        // Не нужно реализовывать этот метод
        public void UpdateOrInsert(UserEntity user, out bool isInserted)
        {
            throw new NotImplementedException();
        }
    }
}