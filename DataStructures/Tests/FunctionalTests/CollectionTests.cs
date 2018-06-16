﻿using System;
using System.Collections.Generic;
using FunctionalTests.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FunctionalTests
{
    public abstract class CollectionTests<T> where T:ICollection<int>
    {
        protected internal abstract T GetCollection(int? capacity = null);
        protected internal abstract void CheckStructure(T target);

        [TestMethod]
        public virtual void AddSimple()
        {
            var target = GetCollection();
            Assert.AreEqual(0, target.Count);

            target.Add(1);
            Assert.AreEqual(1, target.Count);
            Assert.IsTrue(target.Contains(1));

            target.Add(2);
            Assert.AreEqual(2, target.Count);
            Assert.IsTrue(target.Contains(1));
            Assert.IsTrue(target.Contains(2));

            target.Add(3);
            Assert.AreEqual(3, target.Count);
            Assert.IsTrue(target.Contains(1));
            Assert.IsTrue(target.Contains(2));
            Assert.IsTrue(target.Contains(3));
        }


        [TestMethod]
        public virtual void AddRandomized()
        {
            var target = GetCollection();
            Assert.AreEqual(0, target.Count);

            const int count = 1000;
            var random = new Random();
            var store = new bool[count];
            var targetCount = 0;

            // randomly add items from 0 to count-1 to collection and at each step:
            // - update store[i] to true to indicate that item should present in the collection
            // - update expected count
            // - check that collection count is correct
            // - check that items in the collection correspond to items in the store
            for (var i = 0; i < count; i++)
            {
                var item = random.Next(count);
                while (store[item])
                {
                    item = random.Next(count);
                }

                targetCount++;
                store[item] = true;

                target.Add(item);
                Assert.AreEqual(targetCount, target.Count);

                for (var j = 0; j < count; j++)
                {
                    Assert.AreEqual(store[j], target.Contains(j));
                }
                CheckStructure(target);
            }
        }

        [TestMethod]
        public virtual void Clear()
        {
            var target = GetCollection();
            Assert.AreEqual(0, target.Count);

            target.Add(1);
            Assert.AreEqual(1, target.Count);
            Assert.IsTrue(target.Contains(1));

            target.Clear();
            Assert.AreEqual(0, target.Count);
            Assert.IsFalse(target.Contains(1));
        }

        [TestMethod]
        public virtual void CopyTo()
        {
            var target = GetCollection();
            Assert.AreEqual(0, target.Count);

            const int count = 10;
            for (var i = 0; i < count; i++)
            {
                target.Add(i);
            }

            // ReSharper disable once AssignNullToNotNullAttribute
            AssertEx.Throws<ArgumentNullException>(() => target.CopyTo(null, 0));
            AssertEx.Throws<ArgumentOutOfRangeException>(() => target.CopyTo(new int[count], -1));
            AssertEx.Throws<ArgumentException>(() => target.CopyTo(new int[1], 0));

            var result = new int[count];
            target.CopyTo(result, 0);

            for (var i = 0; i < count; i++)
            {
                Assert.AreEqual(i, result[i]);
            }

            result = new int[count + 1];
            result[0] = -1;
            target.CopyTo(result, 1);
            Assert.AreEqual(-1, result[0]);
            for (var i = 0; i < count; i++)
            {
                Assert.AreEqual(i, result[i+1]);
            }
        }

        [TestMethod]
        public virtual void RemoveSimple()
        {
            var target = GetCollection();
            Assert.AreEqual(0, target.Count);

            for (var i = 0; i < 5; i++)
            {
                target.Add(i);
            }
            Assert.AreEqual(5, target.Count);

            Assert.IsTrue(target.Remove(0));
            Assert.AreEqual(4, target.Count);
            Assert.IsFalse(target.Contains(0));

            Assert.IsFalse(target.Remove(0));

            Assert.IsTrue(target.Remove(2));
            Assert.AreEqual(3, target.Count);
            Assert.IsFalse(target.Contains(2));
            Assert.IsFalse(target.Remove(2));

            Assert.IsTrue(target.Remove(1));
            Assert.IsTrue(target.Remove(3));
            Assert.AreEqual(1, target.Count);
            Assert.IsFalse(target.Contains(1));
            Assert.IsFalse(target.Contains(3));
            Assert.IsTrue(target.Contains(4));

            Assert.IsTrue(target.Remove(4));
            Assert.AreEqual(0, target.Count);
            Assert.IsFalse(target.Contains(4));
        }

        [TestMethod]
        public virtual void RemoveRandomized()
        {
            var target = GetCollection();
            Assert.AreEqual(0, target.Count);

            const int count = 1000;
            var random = new Random();
            var store = new bool[count];

            // add items from 0 to count-1 into collection
            // set store[i] to true to indicate that item should be in the collection
            for (var i = 0; i < count; i++)
            {
                target.Add(i);
                store[i] = true;
            }
            var targetCount = count;

            // randomly remove items from collection and at each step:
            // - update the store and expected count
            // - check that count is correct
            // - check that items in the store correspond to items in the collection
            for (var i = 0; i < count; i++)
            {
                var item = random.Next(count);
                while (!store[item])
                {
                    // if item is not in the store it should not be in the collection
                    Assert.IsFalse(target.Remove(item));
                    item = random.Next(count);
                }

                targetCount--;
                store[item] = false;

                target.Remove(item);
                Assert.AreEqual(targetCount, target.Count);

                for (var j = 0; j < count; j++)
                {
                    Assert.AreEqual(store[j], target.Contains(j));
                }
                CheckStructure(target);
            }
        }

        [TestMethod]
        public virtual void GetEnumerator()
        {
            var target = GetCollection();
            Assert.AreEqual(0, target.Count);

            const int count = 10;
            for (var i = 0; i < count; i++)
            {
                target.Add(i);
            }

            var store = new List<int>(count);
            store.AddRange(target);
            var result = store.ToArray();
            Assert.AreEqual(count, result.Length);
            Array.Sort(result);
            for (var i = 0; i < count; i++)
            {
                Assert.AreEqual(i, result[i]);
            }
        }
    }
}
